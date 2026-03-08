"""
Polymarket Yes/No Arbitrage Bot — WebSocket Edition v2.1
=========================================================
Strategi: Beli YES + NO di market yang sama kalau combined ask price < 1.0
Guaranteed profit = 1.0 - (harga_yes + harga_no)

Upgrade v2.1 (bug fixes):
  - Fixed WS subscription format (type: "market", lowercase)
  - Added PING heartbeat setiap 10 detik (required by Polymarket WS)
  - Added custom_feature_enabled untuk best_bid_ask events
  - Fixed get_order_books return type (OrderBookSummary object, bukan dict)
  - Fixed create_order: tambah options param (tick_size, neg_risk)
  - Fixed FOK cancel logic (FOK filled = tidak bisa cancel)
  - Replaced threading.Lock dengan asyncio.Lock (async-safe)
  - Fixed asyncio event loop deprecation

Requirements:
    pip install py-clob-client web3 tenacity python-dotenv websockets

Setup:
    Buat file .env:
        PRIVATE_KEY=0x...
"""

import os
import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Optional, Dict, List
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

import websockets
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BookParams
from py_clob_client.order_builder.constants import BUY

load_dotenv()

# -------------------------------------------------
# KONFIGURASI
# -------------------------------------------------
CONFIG = {
    "MIN_PROFIT_THRESHOLD": 0.01,       # 1% minimum profit
    "MAX_POSITION_SIZE": 10.0,          # Max USDC per sisi
    "MIN_LIQUIDITY": 500.0,             # Min likuiditas per sisi (USDC)
    "REST_REFRESH_INTERVAL": 300,       # Refresh market list via REST (detik)
    "DRY_RUN": True,                    # True = simulasi, False = live
    "MAX_CONSECUTIVE_LOSSES": 3,        # Circuit breaker
    "CHAIN_ID": 137,                    # Polygon mainnet
    "HOST": "https://clob.polymarket.com",
    "WS_URL": "wss://ws-subscriptions-clob.polymarket.com/ws/market",
    "ARB_COOLDOWN": 5,                  # Detik cooldown per market setelah eksekusi
    "MAX_SUBSCRIPTIONS": 200,           # Max token IDs yang di-subscribe sekaligus
    "WS_PING_INTERVAL": 10,             # Heartbeat interval (detik, Polymarket requires 10s)
    "DEFAULT_TICK_SIZE": "0.01",        # Default tick size
}

# -------------------------------------------------
# LOGGING
# -------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("arb_bot.log"),
    ],
)
log = logging.getLogger("polymarket_arb")


# -------------------------------------------------
# DATA CLASSES
# -------------------------------------------------
@dataclass
class OrderBookSide:
    """Representasi satu sisi order book (asks atau bids)."""
    levels: List[Dict] = field(default_factory=list)  # [{price, size}, ...]

    def best_ask(self) -> Optional[float]:
        if not self.levels:
            return None
        asks = sorted(self.levels, key=lambda x: float(x["price"]))
        return float(asks[0]["price"])

    def best_ask_size(self) -> float:
        if not self.levels:
            return 0.0
        asks = sorted(self.levels, key=lambda x: float(x["price"]))
        return float(asks[0]["size"])

    def liquidity_at_top(self, n=5) -> float:
        asks = sorted(self.levels, key=lambda x: float(x["price"]))[:n]
        return sum(float(a["size"]) * float(a["price"]) for a in asks)


@dataclass
class TokenBook:
    token_id: str
    outcome: str          # "YES" atau "NO"
    market_id: str
    neg_risk: bool = False
    tick_size: str = "0.01"
    asks: OrderBookSide = field(default_factory=OrderBookSide)
    last_updated: float = 0.0


@dataclass
class ArbitrageOpportunity:
    market_id: str
    question: str
    yes_token_id: str
    no_token_id: str
    yes_ask: float
    no_ask: float
    yes_ask_size: float
    no_ask_size: float
    combined_cost: float
    profit: float
    profit_pct: float
    neg_risk: bool
    tick_size: str


@dataclass
class TradeResult:
    success: bool
    yes_order_id: Optional[str]
    no_order_id: Optional[str]
    error: Optional[str] = None


# -------------------------------------------------
# ORDER BOOK CACHE (async-safe)
# -------------------------------------------------
class OrderBookCache:
    """Async-safe in-memory cache untuk semua order books."""

    def __init__(self):
        self._books: Dict[str, TokenBook] = {}  # token_id -> TokenBook
        self._lock = asyncio.Lock()

    async def update(self, token_id: str, event_type: str, data: dict):
        async with self._lock:
            if token_id not in self._books:
                return
            book = self._books[token_id]

            if event_type == "book":  # Full snapshot
                book.asks = OrderBookSide(levels=data.get("asks", []))
            elif event_type == "price_change":  # Incremental update
                for change in data.get("changes", []):
                    self._apply_price_change(book.asks, change)

            book.last_updated = time.time()

    def _apply_price_change(self, side: OrderBookSide, change: dict):
        price = change.get("price")
        size = float(change.get("size", 0))
        side.levels = [l for l in side.levels if l["price"] != price]
        if size > 0:
            side.levels.append({"price": price, "size": str(size)})

    async def register(self, token: TokenBook):
        async with self._lock:
            self._books[token.token_id] = token

    async def update_from_rest(self, asset_id: str, asks: list):
        """Update cache dari REST OrderBookSummary."""
        async with self._lock:
            if asset_id in self._books:
                self._books[asset_id].asks = OrderBookSide(
                    levels=[{"price": a.price, "size": a.size} for a in asks]
                )
                self._books[asset_id].last_updated = time.time()

    async def get(self, token_id: str) -> Optional[TokenBook]:
        async with self._lock:
            book = self._books.get(token_id)
            # Return a shallow copy to avoid race conditions
            if book:
                return TokenBook(
                    token_id=book.token_id,
                    outcome=book.outcome,
                    market_id=book.market_id,
                    neg_risk=book.neg_risk,
                    tick_size=book.tick_size,
                    asks=OrderBookSide(levels=list(book.asks.levels)),
                    last_updated=book.last_updated,
                )
            return None

    async def all_token_ids(self) -> List[str]:
        async with self._lock:
            return list(self._books.keys())

    async def get_market_tokens(self, market_id: str):
        """Return (yes_book, no_book) untuk suatu market."""
        async with self._lock:
            tokens = [b for b in self._books.values() if b.market_id == market_id]
            yes = next((t for t in tokens if t.outcome == "YES"), None)
            no = next((t for t in tokens if t.outcome == "NO"), None)
            # Return copies
            yes_copy = None
            no_copy = None
            if yes:
                yes_copy = TokenBook(
                    token_id=yes.token_id, outcome=yes.outcome,
                    market_id=yes.market_id, neg_risk=yes.neg_risk,
                    tick_size=yes.tick_size,
                    asks=OrderBookSide(levels=list(yes.asks.levels)),
                    last_updated=yes.last_updated,
                )
            if no:
                no_copy = TokenBook(
                    token_id=no.token_id, outcome=no.outcome,
                    market_id=no.market_id, neg_risk=no.neg_risk,
                    tick_size=no.tick_size,
                    asks=OrderBookSide(levels=list(no.asks.levels)),
                    last_updated=no.last_updated,
                )
            return yes_copy, no_copy

    async def all_market_ids(self) -> List[str]:
        async with self._lock:
            return list({b.market_id for b in self._books.values()})


# -------------------------------------------------
# WEBSOCKET MANAGER
# -------------------------------------------------
class WebSocketManager:
    """Kelola koneksi WebSocket ke Polymarket dan update cache."""

    def __init__(self, cache: OrderBookCache, token_ids: List[str]):
        self.cache = cache
        self.token_ids = token_ids
        self._running = False

    async def connect(self):
        self._running = True
        while self._running:
            try:
                log.info("Menghubungkan ke Polymarket WebSocket...")
                async with websockets.connect(
                    CONFIG["WS_URL"],
                    ping_interval=None,   # Kita handle ping manual
                    ping_timeout=None,
                ) as ws:
                    log.info("WebSocket terhubung. Subscribe ke %d token...", len(self.token_ids))
                    await self._subscribe(ws)
                    # Jalankan listener dan heartbeat secara concurrent
                    await asyncio.gather(
                        self._listen(ws),
                        self._heartbeat(ws),
                    )
            except websockets.exceptions.ConnectionClosed as e:
                log.warning("WebSocket terputus: %s. Reconnect dalam 3 detik...", e)
                await asyncio.sleep(3)
            except Exception as e:
                log.error("WebSocket error: %s. Reconnect dalam 5 detik...", e)
                await asyncio.sleep(5)

    async def _subscribe(self, ws):
        """Subscribe ke market channel.
        Format: {assets_ids: [...], type: "market", custom_feature_enabled: true}
        Batch per 100 token IDs.
        """
        BATCH = 100
        for i in range(0, len(self.token_ids), BATCH):
            batch = self.token_ids[i:i + BATCH]
            msg = json.dumps({
                "assets_ids": batch,
                "type": "market",
                "custom_feature_enabled": True,
            })
            await ws.send(msg)
            await asyncio.sleep(0.1)
        log.info("Subscribed ke %d token.", len(self.token_ids))

    async def _heartbeat(self, ws):
        """Kirim PING text setiap 10 detik (required oleh Polymarket WS)."""
        while self._running:
            try:
                await ws.send("PING")
            except Exception:
                break  # Connection lost, will reconnect from connect()
            await asyncio.sleep(CONFIG["WS_PING_INTERVAL"])

    async def _listen(self, ws):
        async for raw in ws:
            try:
                # Ignore PONG responses
                if raw == "PONG":
                    continue

                events = json.loads(raw)
                if not isinstance(events, list):
                    events = [events]
                for event in events:
                    await self._process_event(event)
            except json.JSONDecodeError:
                # Non-JSON message (like PONG), skip
                continue
            except Exception as e:
                log.debug("Error parse WS event: %s", e)

    async def _process_event(self, event: dict):
        event_type = event.get("event_type", "")
        asset_id = event.get("asset_id", "")
        if not asset_id:
            return
        await self.cache.update(asset_id, event_type, event)

    def stop(self):
        self._running = False


# -------------------------------------------------
# BOT UTAMA
# -------------------------------------------------
class PolymarketArbBot:

    def __init__(self):
        self.private_key = os.getenv("PRIVATE_KEY")
        if not self.private_key:
            raise ValueError("PRIVATE_KEY tidak ditemukan di .env")

        self.client = ClobClient(
            CONFIG["HOST"],
            key=self.private_key,
            chain_id=CONFIG["CHAIN_ID"],
            signature_type=0,
        )
        log.info("Menghubungkan ke Polymarket REST API...")
        api_creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(api_creds)
        log.info("REST API terhubung.")

        self.cache = OrderBookCache()
        self.ws_manager: Optional[WebSocketManager] = None

        # Arb engine state
        self._arb_lock = asyncio.Lock()
        self._cooldowns: Dict[str, float] = {}  # market_id -> timestamp
        self.consecutive_losses = 0
        self.total_trades = 0
        self.total_profit = 0.0
        self.total_opportunities = 0

    # --------------------------------------------------
    # MARKET DISCOVERY (REST)
    # --------------------------------------------------
    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def fetch_markets(self):
        all_markets = []
        next_cursor = None
        while True:
            params = {"active": True, "closed": False}
            if next_cursor:
                params["next_cursor"] = next_cursor
            response = self.client.get_markets(**params)
            data = response.get("data", []) if isinstance(response, dict) else []
            all_markets.extend(data)
            next_cursor = response.get("next_cursor") if isinstance(response, dict) else None
            if not next_cursor or next_cursor == "LTE=":
                break
        log.info("Fetched %d market aktif.", len(all_markets))
        return all_markets

    async def register_markets(self, markets) -> List[str]:
        """Daftarkan token ke cache dan return list token_ids."""
        token_ids = []
        for market in markets:
            tokens = market.get("tokens", [])
            if len(tokens) != 2:
                continue
            yes_token = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), None)
            no_token = next((t for t in tokens if t.get("outcome", "").upper() == "NO"), None)
            if not yes_token or not no_token:
                continue

            market_id = market.get("condition_id", "")
            neg_risk = market.get("neg_risk", False)
            tick_size = str(market.get("minimum_tick_size", CONFIG["DEFAULT_TICK_SIZE"]))

            await self.cache.register(TokenBook(
                token_id=yes_token["token_id"],
                outcome="YES",
                market_id=market_id,
                neg_risk=neg_risk,
                tick_size=tick_size,
            ))
            await self.cache.register(TokenBook(
                token_id=no_token["token_id"],
                outcome="NO",
                market_id=market_id,
                neg_risk=neg_risk,
                tick_size=tick_size,
            ))
            token_ids.append(yes_token["token_id"])
            token_ids.append(no_token["token_id"])

        log.info("Registered %d token dari %d market.", len(token_ids), len(markets))
        return token_ids[:CONFIG["MAX_SUBSCRIPTIONS"] * 2]

    async def prime_cache_from_rest(self, token_ids: List[str]):
        """Isi cache awal dengan data REST sebelum WS nyambung.
        Note: get_order_books returns OrderBookSummary objects, not dicts.
        """
        log.info("Priming cache dari REST API...")
        BATCH = 20
        loop = asyncio.get_running_loop()
        for i in range(0, len(token_ids), BATCH):
            batch = token_ids[i:i + BATCH]
            try:
                # get_order_books is sync, run in executor
                books = await loop.run_in_executor(
                    None,
                    lambda b=batch: self.client.get_order_books(
                        [BookParams(token_id=tid) for tid in b]
                    )
                )
                for book in books:
                    # OrderBookSummary has .asset_id and .asks attributes
                    asset_id = getattr(book, "asset_id", "") or ""
                    asks = getattr(book, "asks", []) or []
                    if asset_id and asks:
                        await self.cache.update_from_rest(asset_id, asks)
            except Exception as e:
                log.warning("REST prime batch error: %s", e)
        log.info("Cache primed.")

    # --------------------------------------------------
    # ARB ENGINE
    # --------------------------------------------------
    async def check_market_arb(self, market_id: str) -> Optional[ArbitrageOpportunity]:
        yes_book, no_book = await self.cache.get_market_tokens(market_id)
        if not yes_book or not no_book:
            return None

        yes_ask = yes_book.asks.best_ask()
        no_ask = no_book.asks.best_ask()
        if yes_ask is None or no_ask is None:
            return None

        yes_liq = yes_book.asks.liquidity_at_top()
        no_liq = no_book.asks.liquidity_at_top()
        min_liq = CONFIG["MIN_LIQUIDITY"]
        if yes_liq < min_liq or no_liq < min_liq:
            return None

        combined = yes_ask + no_ask
        profit = 1.0 - combined

        if profit > CONFIG["MIN_PROFIT_THRESHOLD"]:
            return ArbitrageOpportunity(
                market_id=market_id,
                question="",
                yes_token_id=yes_book.token_id,
                no_token_id=no_book.token_id,
                yes_ask=yes_ask,
                no_ask=no_ask,
                yes_ask_size=yes_book.asks.best_ask_size(),
                no_ask_size=no_book.asks.best_ask_size(),
                combined_cost=combined,
                profit=profit,
                profit_pct=profit * 100,
                neg_risk=yes_book.neg_risk,
                tick_size=yes_book.tick_size,
            )
        return None

    def _is_on_cooldown(self, market_id: str) -> bool:
        last = self._cooldowns.get(market_id, 0)
        return (time.time() - last) < CONFIG["ARB_COOLDOWN"]

    def _set_cooldown(self, market_id: str):
        self._cooldowns[market_id] = time.time()

    async def scan_all_markets(self):
        """Scan seluruh cache untuk peluang arb."""
        opportunities = []
        for market_id in await self.cache.all_market_ids():
            opp = await self.check_market_arb(market_id)
            if opp and not self._is_on_cooldown(market_id):
                opportunities.append(opp)
        opportunities.sort(key=lambda x: x.profit, reverse=True)
        return opportunities

    # --------------------------------------------------
    # EKSEKUSI TRADE
    # --------------------------------------------------
    def _create_and_post_fok(self, token_id: str, price: float,
                              size_usdc: float, neg_risk: bool,
                              tick_size: str) -> dict:
        """Create signed order and post as FOK. Returns response dict.
        Runs in sync context (called via run_in_executor).
        """
        shares = round(size_usdc / price, 2)
        signed = self.client.create_order(
            OrderArgs(
                token_id=token_id,
                price=price,
                size=shares,
                side=BUY,
            ),
            options={"tick_size": tick_size, "neg_risk": neg_risk},
        )
        return self.client.post_order(signed, OrderType.FOK)

    async def execute_arbitrage(self, opp: ArbitrageOpportunity) -> TradeResult:
        if CONFIG["DRY_RUN"]:
            log.info(
                "[DRY RUN] YES @ %.4f + NO @ %.4f | Profit: %.2f%% | Market: %s",
                opp.yes_ask, opp.no_ask, opp.profit_pct, opp.market_id[:16]
            )
            self._set_cooldown(opp.market_id)
            self.total_opportunities += 1
            return TradeResult(success=True, yes_order_id="DRY_YES", no_order_id="DRY_NO")

        size = CONFIG["MAX_POSITION_SIZE"]
        loop = asyncio.get_running_loop()
        yes_order_id = None

        try:
            # --- Leg 1: BUY YES (FOK) ---
            yes_resp = await loop.run_in_executor(
                None,
                lambda: self._create_and_post_fok(
                    opp.yes_token_id, opp.yes_ask, size,
                    opp.neg_risk, opp.tick_size
                )
            )
            yes_order_id = yes_resp.get("orderID")
            yes_status = yes_resp.get("status", "")

            # FOK: kalau status bukan "matched" berarti gagal (tidak terisi)
            if not yes_order_id or yes_status not in ("matched", "MATCHED"):
                raise Exception(f"Order YES gagal (FOK not filled): {yes_resp}")
            log.info("Order YES OK (FOK filled): %s", yes_order_id)

            # --- Leg 2: BUY NO (FOK) ---
            no_resp = await loop.run_in_executor(
                None,
                lambda: self._create_and_post_fok(
                    opp.no_token_id, opp.no_ask, size,
                    opp.neg_risk, opp.tick_size
                )
            )
            no_order_id = no_resp.get("orderID")
            no_status = no_resp.get("status", "")

            if not no_order_id or no_status not in ("matched", "MATCHED"):
                # FOK YES sudah terisi = TIDAK bisa cancel.
                # Log sebagai partial fill — perlu manual intervention atau
                # jual YES kembali sebagai exit strategy.
                log.error(
                    "PARTIAL FILL! YES terisi (%s) tapi NO gagal. "
                    "YES position terbuka — perlu jual manual atau tunggu resolusi. "
                    "NO response: %s",
                    yes_order_id, no_resp
                )
                raise Exception(f"Order NO gagal (FOK not filled): {no_resp}")

            log.info("Order NO OK (FOK filled): %s", no_order_id)
            self.total_trades += 1
            self.total_profit += opp.profit * size
            self.consecutive_losses = 0
            self._set_cooldown(opp.market_id)
            return TradeResult(success=True, yes_order_id=yes_order_id, no_order_id=no_order_id)

        except Exception as e:
            self.consecutive_losses += 1
            log.error("Trade gagal: %s", e)
            return TradeResult(
                success=False, yes_order_id=yes_order_id,
                no_order_id=None, error=str(e)
            )

    # --------------------------------------------------
    # MAIN LOOPS (ASYNC)
    # --------------------------------------------------
    async def arb_loop(self):
        """Scan semua market setiap 0.5 detik."""
        log.info("Arb engine started.")
        while True:
            try:
                if self.consecutive_losses >= CONFIG["MAX_CONSECUTIVE_LOSSES"]:
                    log.warning(
                        "Circuit breaker: %d consecutive losses. Pause 5 menit...",
                        self.consecutive_losses
                    )
                    await asyncio.sleep(300)
                    self.consecutive_losses = 0
                    continue

                opportunities = await self.scan_all_markets()
                if opportunities:
                    for opp in opportunities:
                        async with self._arb_lock:
                            result = await self.execute_arbitrage(opp)
                        if result.success:
                            log.info(
                                "Trade sukses | YES: %s NO: %s",
                                result.yes_order_id, result.no_order_id
                            )
                        else:
                            log.warning("Trade gagal: %s", result.error)

            except Exception as e:
                log.error("Arb loop error: %s", e)

            await asyncio.sleep(0.5)

    async def stats_loop(self):
        """Print statistik setiap 60 detik."""
        while True:
            await asyncio.sleep(60)
            token_count = len(await self.cache.all_token_ids())
            market_count = len(await self.cache.all_market_ids())
            log.info(
                "STATS | Markets: %d | Tokens: %d | Trades: %d | "
                "Opportunities: %d | Profit: $%.4f USDC | Consecutive losses: %d",
                market_count, token_count,
                self.total_trades, self.total_opportunities,
                self.total_profit, self.consecutive_losses
            )

    async def rest_refresh_loop(self, token_ids: List[str]):
        """Refresh cache dari REST secara periodik sebagai fallback."""
        while True:
            await asyncio.sleep(CONFIG["REST_REFRESH_INTERVAL"])
            log.info("REST refresh: re-priming cache...")
            await self.prime_cache_from_rest(token_ids)

    async def run_async(self):
        log.info("=" * 60)
        log.info("Polymarket Arb Bot v2.1 — WebSocket Edition")
        log.info("Mode: %s", "DRY RUN" if CONFIG["DRY_RUN"] else "*** LIVE TRADING ***")
        log.info(
            "Min profit: %.1f%% | Max size: $%.2f | Cooldown: %ds",
            CONFIG["MIN_PROFIT_THRESHOLD"] * 100,
            CONFIG["MAX_POSITION_SIZE"],
            CONFIG["ARB_COOLDOWN"],
        )
        log.info("=" * 60)

        # 1. Fetch & register markets (sync call in executor)
        loop = asyncio.get_running_loop()
        markets = await loop.run_in_executor(None, self.fetch_markets)
        token_ids = await self.register_markets(markets)

        if not token_ids:
            log.error("Tidak ada market valid ditemukan. Exit.")
            return

        # 2. Prime cache dari REST
        await self.prime_cache_from_rest(token_ids)

        # 3. Start WebSocket manager
        self.ws_manager = WebSocketManager(self.cache, token_ids)

        # 4. Jalankan semua loop secara concurrent
        log.info("Starting 4 concurrent loops...")
        await asyncio.gather(
            self.ws_manager.connect(),         # WS real-time feed + heartbeat
            self.arb_loop(),                   # Arb engine
            self.stats_loop(),                 # Stats printer
            self.rest_refresh_loop(token_ids), # REST fallback
        )

    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            log.info("Bot dihentikan oleh user.")
        finally:
            log.info(
                "FINAL STATS | Trades: %d | Profit: $%.4f USDC",
                self.total_trades, self.total_profit
            )


# -------------------------------------------------
# ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    bot = PolymarketArbBot()
    bot.run()
