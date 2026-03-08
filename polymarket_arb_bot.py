"""
Polymarket Yes/No Arbitrage Bot — WebSocket Edition
=====================================================
Strategi: Beli YES + NO di market yang sama kalau combined ask price < 1.0
Guaranteed profit = 1.0 - (harga_yes + harga_no)

Upgrade v2:
  - WebSocket real-time price feed (Polymarket WS API)
  - Local order book cache — tidak perlu polling REST setiap detik
  - Arb engine triggered on every price update (event-driven)
  - REST polling tetap ada sebagai fallback & market discovery

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
import threading
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
        asks = sorted(self.levels, key=lambda x: float(x["price"]))
        return float(asks[0]["price"]) if asks else None

    def liquidity_at_top(self, n=5) -> float:
        asks = sorted(self.levels, key=lambda x: float(x["price"]))[:n]
        return sum(float(a["size"]) * float(a["price"]) for a in asks)


@dataclass
class TokenBook:
    token_id: str
    outcome: str          # "YES" atau "NO"
    market_id: str
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
    combined_cost: float
    profit: float
    profit_pct: float


@dataclass
class TradeResult:
    success: bool
    yes_order_id: Optional[str]
    no_order_id: Optional[str]
    error: Optional[str] = None


# -------------------------------------------------
# ORDER BOOK CACHE
# -------------------------------------------------
class OrderBookCache:
    """Thread-safe in-memory cache untuk semua order books."""

    def __init__(self):
        self._books: Dict[str, TokenBook] = {}  # token_id -> TokenBook
        self._lock = threading.Lock()

    def update(self, token_id: str, event_type: str, data: dict):
        with self._lock:
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

    def register(self, token: TokenBook):
        with self._lock:
            self._books[token.token_id] = token

    def get(self, token_id: str) -> Optional[TokenBook]:
        with self._lock:
            return self._books.get(token_id)

    def all_token_ids(self) -> List[str]:
        with self._lock:
            return list(self._books.keys())

    def get_market_tokens(self, market_id: str):
        """Return (yes_book, no_book) untuk suatu market."""
        with self._lock:
            tokens = [b for b in self._books.values() if b.market_id == market_id]
            yes = next((t for t in tokens if t.outcome == "YES"), None)
            no = next((t for t in tokens if t.outcome == "NO"), None)
            return yes, no

    def all_market_ids(self) -> List[str]:
        with self._lock:
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
                    ping_interval=20,
                    ping_timeout=30,
                ) as ws:
                    log.info("WebSocket terhubung. Subscribe ke %d token...", len(self.token_ids))
                    await self._subscribe(ws)
                    await self._listen(ws)
            except websockets.exceptions.ConnectionClosed as e:
                log.warning("WebSocket terputus: %s. Reconnect dalam 3 detik...", e)
                await asyncio.sleep(3)
            except Exception as e:
                log.error("WebSocket error: %s. Reconnect dalam 5 detik...", e)
                await asyncio.sleep(5)

    async def _subscribe(self, ws):
        # Polymarket WS: subscribe dalam batch
        BATCH = 100
        for i in range(0, len(self.token_ids), BATCH):
            batch = self.token_ids[i:i + BATCH]
            msg = json.dumps({
                "auth": {},
                "markets": [],
                "assets_ids": batch,
                "type": "Market",
            })
            await ws.send(msg)
            await asyncio.sleep(0.1)

    async def _listen(self, ws):
        async for raw in ws:
            try:
                events = json.loads(raw)
                if not isinstance(events, list):
                    events = [events]
                for event in events:
                    self._process_event(event)
            except Exception as e:
                log.debug("Error parse WS event: %s", e)

    def _process_event(self, event: dict):
        event_type = event.get("event_type", "")
        asset_id = event.get("asset_id", "")
        if not asset_id:
            return
        self.cache.update(asset_id, event_type, event)

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
            all_markets.extend(response.get("data", []))
            next_cursor = response.get("next_cursor")
            if not next_cursor or next_cursor == "LTE=":
                break
        log.info("Fetched %d market aktif.", len(all_markets))
        return all_markets

    def register_markets(self, markets) -> List[str]:
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
            question = market.get("question", "Unknown")

            self.cache.register(TokenBook(
                token_id=yes_token["token_id"],
                outcome="YES",
                market_id=market_id,
            ))
            self.cache.register(TokenBook(
                token_id=no_token["token_id"],
                outcome="NO",
                market_id=market_id,
            ))
            token_ids.append(yes_token["token_id"])
            token_ids.append(no_token["token_id"])

        log.info("Registered %d token dari %d market.", len(token_ids), len(markets))
        return token_ids[:CONFIG["MAX_SUBSCRIPTIONS"] * 2]

    def prime_cache_from_rest(self, token_ids: List[str]):
        """Isi cache awal dengan data REST sebelum WS nyambung."""
        log.info("Priming cache dari REST API...")
        BATCH = 20
        for i in range(0, len(token_ids), BATCH):
            batch = token_ids[i:i + BATCH]
            try:
                books = self.client.get_order_books(
                    [BookParams(token_id=tid) for tid in batch]
                )
                for book in books:
                    asset_id = book.get("asset_id", "")
                    if asset_id:
                        self.cache.update(asset_id, "book", book)
            except Exception as e:
                log.warning("REST prime batch error: %s", e)
        log.info("Cache primed.")

    # --------------------------------------------------
    # ARB ENGINE (dipanggil event-driven setiap update WS)
    # --------------------------------------------------
    def check_market_arb(self, market_id: str) -> Optional[ArbitrageOpportunity]:
        yes_book, no_book = self.cache.get_market_tokens(market_id)
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
                combined_cost=combined,
                profit=profit,
                profit_pct=profit * 100,
            )
        return None

    def _is_on_cooldown(self, market_id: str) -> bool:
        last = self._cooldowns.get(market_id, 0)
        return (time.time() - last) < CONFIG["ARB_COOLDOWN"]

    def _set_cooldown(self, market_id: str):
        self._cooldowns[market_id] = time.time()

    def scan_all_markets(self):
        """Scan seluruh cache untuk peluang arb (dipanggil periodik sebagai fallback)."""
        opportunities = []
        for market_id in self.cache.all_market_ids():
            opp = self.check_market_arb(market_id)
            if opp and not self._is_on_cooldown(market_id):
                opportunities.append(opp)
        opportunities.sort(key=lambda x: x.profit, reverse=True)
        return opportunities

    # --------------------------------------------------
    # EKSEKUSI TRADE
    # --------------------------------------------------
    def execute_arbitrage(self, opp: ArbitrageOpportunity) -> TradeResult:
        if CONFIG["DRY_RUN"]:
            log.info(
                "[DRY RUN] YES @ %.4f + NO @ %.4f | Profit: %.2f%% | Market: %s",
                opp.yes_ask, opp.no_ask, opp.profit_pct, opp.market_id[:16]
            )
            self._set_cooldown(opp.market_id)
            return TradeResult(success=True, yes_order_id="DRY_YES", no_order_id="DRY_NO")

        size = CONFIG["MAX_POSITION_SIZE"]
        yes_order_id = None

        try:
            yes_signed = self.client.create_order(OrderArgs(
                token_id=opp.yes_token_id,
                price=opp.yes_ask,
                size=round(size / opp.yes_ask, 2),
                side=BUY,
            ))
            yes_resp = self.client.post_order(yes_signed, OrderType.FOK)
            yes_order_id = yes_resp.get("orderID")
            if not yes_order_id or yes_resp.get("status") == "unmatched":
                raise Exception("Order YES gagal: " + str(yes_resp))
            log.info("Order YES OK: %s", yes_order_id)

            no_signed = self.client.create_order(OrderArgs(
                token_id=opp.no_token_id,
                price=opp.no_ask,
                size=round(size / opp.no_ask, 2),
                side=BUY,
            ))
            no_resp = self.client.post_order(no_signed, OrderType.FOK)
            no_order_id = no_resp.get("orderID")
            if not no_order_id or no_resp.get("status") == "unmatched":
                log.warning("Order NO gagal, cancel YES (%s)...", yes_order_id)
                try:
                    self.client.cancel(order_id=yes_order_id)
                except Exception as ce:
                    log.error("Gagal cancel YES! Manual action: %s", ce)
                raise Exception("Order NO gagal: " + str(no_resp))

            log.info("Order NO OK: %s", no_order_id)
            self.total_trades += 1
            self.total_profit += opp.profit * size
            self.consecutive_losses = 0
            self._set_cooldown(opp.market_id)
            return TradeResult(success=True, yes_order_id=yes_order_id, no_order_id=no_order_id)

        except Exception as e:
            self.consecutive_losses += 1
            log.error("Trade gagal: %s", e)
            return TradeResult(success=False, yes_order_id=yes_order_id, no_order_id=None, error=str(e))

    # --------------------------------------------------
    # MAIN LOOP (ASYNC)
    # --------------------------------------------------
    async def arb_loop(self):
        """Event-driven arb loop — scan semua market setiap 0.5 detik."""
        log.info("Arb engine started (event-driven mode).")
        while True:
            try:
                if self.consecutive_losses >= CONFIG["MAX_CONSECUTIVE_LOSSES"]:
                    log.warning("Circuit breaker: %d consecutive losses. Pause 5 menit...",
                                self.consecutive_losses)
                    await asyncio.sleep(300)
                    self.consecutive_losses = 0
                    continue

                opportunities = self.scan_all_markets()
                if opportunities:
                    self.total_opportunities += len(opportunities)
                    for opp in opportunities:
                        async with self._arb_lock:
                            result = self.execute_arbitrage(opp)
                        if result.success:
                            log.info("Trade sukses | YES: %s NO: %s",
                                     result.yes_order_id, result.no_order_id)
                        else:
                            log.warning("Trade gagal: %s", result.error)

            except Exception as e:
                log.error("Arb loop error: %s", e)

            await asyncio.sleep(0.5)

    async def stats_loop(self):
        """Print statistik setiap 60 detik."""
        while True:
            await asyncio.sleep(60)
            log.info(
                "STATS | Trades: %d | Opportunities: %d | Profit: $%.4f USDC | Losses: %d",
                self.total_trades, self.total_opportunities,
                self.total_profit, self.consecutive_losses
            )

    async def rest_refresh_loop(self, token_ids: List[str]):
        """Refresh cache dari REST secara periodik sebagai fallback."""
        while True:
            await asyncio.sleep(CONFIG["REST_REFRESH_INTERVAL"])
            log.info("REST refresh: re-priming cache...")
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(None, self.prime_cache_from_rest, token_ids)

    async def run_async(self):
        log.info("=" * 60)
        log.info("Polymarket Arb Bot v2 — WebSocket Edition")
        log.info("Mode: %s", "DRY RUN" if CONFIG["DRY_RUN"] else "*** LIVE TRADING ***")
        log.info("Min profit: %.1f%% | Max size: $%.2f | Cooldown: %ds",
                 CONFIG["MIN_PROFIT_THRESHOLD"] * 100,
                 CONFIG["MAX_POSITION_SIZE"],
                 CONFIG["ARB_COOLDOWN"])
        log.info("=" * 60)

        # 1. Fetch & register markets
        markets = await asyncio.get_event_loop().run_in_executor(None, self.fetch_markets)
        token_ids = self.register_markets(markets)

        # 2. Prime cache dari REST
        await asyncio.get_event_loop().run_in_executor(
            None, self.prime_cache_from_rest, token_ids
        )

        # 3. Start WebSocket manager
        self.ws_manager = WebSocketManager(self.cache, token_ids)

        # 4. Jalankan semua loop secara concurrent
        await asyncio.gather(
            self.ws_manager.connect(),         # WS real-time feed
            self.arb_loop(),                   # Arb engine
            self.stats_loop(),                 # Stats printer
            self.rest_refresh_loop(token_ids), # REST fallback
        )

    def run(self):
        try:
            asyncio.run(self.run_async())
        except KeyboardInterrupt:
            log.info("Bot dihentikan.")
            log.info("FINAL STATS | Trades: %d | Profit: $%.4f USDC",
                     self.total_trades, self.total_profit)


# -------------------------------------------------
# ENTRY POINT
# -------------------------------------------------
if __name__ == "__main__":
    bot = PolymarketArbBot()
    bot.run()
