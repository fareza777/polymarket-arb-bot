# Polymarket Yes/No Arbitrage Bot
# Strategi: Beli YES + NO di market yang sama kalau combined ask price < 1.0
# Guaranteed profit = 1.0 - (harga_yes + harga_no)
#
# Requirements:
#     pip install py-clob-client web3 tenacity python-dotenv
#
# Setup:
#     Buat file .env dengan:
#         PRIVATE_KEY=0x...

import os
import time
import logging
from dataclasses import dataclass
from typing import Optional
from dotenv import load_dotenv
from tenacity import retry, stop_after_attempt, wait_exponential

from py_clob_client.client import ClobClient
from py_clob_client.clob_types import OrderArgs, OrderType, BookParams
from py_clob_client.order_builder.constants import BUY

load_dotenv()

CONFIG = {
    "MIN_PROFIT_THRESHOLD": 0.01,
    "MAX_POSITION_SIZE": 10.0,
    "MIN_LIQUIDITY": 1000.0,
    "SCAN_INTERVAL": 15,
    "DRY_RUN": True,
    "MAX_CONSECUTIVE_LOSSES": 3,
    "CHAIN_ID": 137,
    "HOST": "https://clob.polymarket.com",
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("arb_bot.log"),
    ],
)
log = logging.getLogger("polymarket_arb")


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


class PolymarketArbBot:

    def __init__(self):
        self.private_key = os.getenv("PRIVATE_KEY")
        if not self.private_key:
            raise ValueError("PRIVATE_KEY tidak ditemukan di environment. Cek file .env kamu.")

        self.client = ClobClient(
            CONFIG["HOST"],
            key=self.private_key,
            chain_id=CONFIG["CHAIN_ID"],
            signature_type=0,
        )

        log.info("Menghubungkan ke Polymarket...")
        api_creds = self.client.create_or_derive_api_creds()
        self.client.set_api_creds(api_creds)
        log.info("Terhubung ke Polymarket CLOB.")

        self.total_scanned = 0
        self.total_opportunities = 0
        self.total_trades = 0
        self.total_profit = 0.0
        self.consecutive_losses = 0

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=10))
    def fetch_markets(self):
        all_markets = []
        next_cursor = None

        while True:
            params = {"active": True, "closed": False}
            if next_cursor:
                params["next_cursor"] = next_cursor

            response = self.client.get_markets(**params)
            markets = response.get("data", [])
            all_markets.extend(markets)

            next_cursor = response.get("next_cursor")
            if not next_cursor or next_cursor == "LTE=":
                break

        log.info("Berhasil fetch %d market aktif.", len(all_markets))
        return all_markets

    def check_arbitrage(self, market):
        try:
            tokens = market.get("tokens", [])
            if len(tokens) != 2:
                return None

            yes_token = next((t for t in tokens if t.get("outcome", "").upper() == "YES"), None)
            no_token = next((t for t in tokens if t.get("outcome", "").upper() == "NO"), None)

            if not yes_token or not no_token:
                return None

            yes_token_id = yes_token["token_id"]
            no_token_id = no_token["token_id"]

            books = self.client.get_order_books([
                BookParams(token_id=yes_token_id),
                BookParams(token_id=no_token_id),
            ])

            if len(books) < 2:
                return None

            yes_book, no_book = books[0], books[1]
            yes_asks = yes_book.get("asks", [])
            no_asks = no_book.get("asks", [])

            if not yes_asks or not no_asks:
                return None

            yes_ask = float(yes_asks[0]["price"])
            no_ask = float(no_asks[0]["price"])

            yes_liquidity = sum(float(a["size"]) * float(a["price"]) for a in yes_asks[:5])
            no_liquidity = sum(float(a["size"]) * float(a["price"]) for a in no_asks[:5])

            min_liq = CONFIG["MIN_LIQUIDITY"] / 2
            if yes_liquidity < min_liq or no_liquidity < min_liq:
                return None

            combined_cost = yes_ask + no_ask
            profit = 1.0 - combined_cost
            profit_pct = profit * 100

            if profit > CONFIG["MIN_PROFIT_THRESHOLD"]:
                return ArbitrageOpportunity(
                    market_id=market.get("condition_id", ""),
                    question=market.get("question", "Unknown"),
                    yes_token_id=yes_token_id,
                    no_token_id=no_token_id,
                    yes_ask=yes_ask,
                    no_ask=no_ask,
                    combined_cost=combined_cost,
                    profit=profit,
                    profit_pct=profit_pct,
                )

        except Exception as e:
            log.debug("Error check market %s: %s", market.get("condition_id", ""), e)

        return None

    def execute_arbitrage(self, opp):
        if CONFIG["DRY_RUN"]:
            log.info(
                "[DRY RUN] YES @ %.4f + NO @ %.4f = %.4f | Profit: %.2f%%",
                opp.yes_ask, opp.no_ask, opp.combined_cost, opp.profit_pct
            )
            return TradeResult(success=True, yes_order_id="DRY_YES", no_order_id="DRY_NO")

        yes_order_id = None
        no_order_id = None
        position_size = CONFIG["MAX_POSITION_SIZE"]

        try:
            yes_signed = self.client.create_order(OrderArgs(
                token_id=opp.yes_token_id,
                price=opp.yes_ask,
                size=round(position_size / opp.yes_ask, 2),
                side=BUY,
            ))
            yes_resp = self.client.post_order(yes_signed, OrderType.FOK)
            yes_order_id = yes_resp.get("orderID")

            if not yes_order_id or yes_resp.get("status") == "unmatched":
                raise Exception("Order YES gagal: " + str(yes_resp))

            log.info("Order YES berhasil: %s", yes_order_id)

            no_signed = self.client.create_order(OrderArgs(
                token_id=opp.no_token_id,
                price=opp.no_ask,
                size=round(position_size / opp.no_ask, 2),
                side=BUY,
            ))
            no_resp = self.client.post_order(no_signed, OrderType.FOK)
            no_order_id = no_resp.get("orderID")

            if not no_order_id or no_resp.get("status") == "unmatched":
                log.warning("Order NO gagal. Cancel order YES (%s)...", yes_order_id)
                try:
                    self.client.cancel(order_id=yes_order_id)
                    log.info("Order YES berhasil di-cancel.")
                except Exception as cancel_err:
                    log.error("Gagal cancel order YES! Manual action diperlukan: %s", cancel_err)
                raise Exception("Order NO gagal: " + str(no_resp))

            log.info("Order NO berhasil: %s", no_order_id)

            self.total_trades += 1
            self.total_profit += opp.profit * position_size
            self.consecutive_losses = 0

            return TradeResult(success=True, yes_order_id=yes_order_id, no_order_id=no_order_id)

        except Exception as e:
            self.consecutive_losses += 1
            log.error("Trade gagal: %s", e)
            return TradeResult(
                success=False,
                yes_order_id=yes_order_id,
                no_order_id=no_order_id,
                error=str(e)
            )

    def check_balance(self):
        try:
            balance_wei = self.client.get_balance()
            return float(balance_wei) / 1e6
        except Exception as e:
            log.warning("Gagal cek balance: %s", e)
            return 0.0

    def print_stats(self):
        log.info("-" * 50)
        log.info(
            "STATS | Scanned: %d | Opportunities: %d | Trades: %d | Profit: $%.4f USDC",
            self.total_scanned, self.total_opportunities,
            self.total_trades, self.total_profit
        )
        log.info("-" * 50)

    def run(self):
        log.info("=" * 50)
        log.info("Polymarket Yes/No Arbitrage Bot STARTED")
        if CONFIG["DRY_RUN"]:
            log.info("Mode: DRY RUN (simulasi, tidak ada order nyata)")
        else:
            log.info("Mode: *** LIVE TRADING ***")
        log.info("Min profit threshold: %.1f%%", CONFIG["MIN_PROFIT_THRESHOLD"] * 100)
        log.info("Max position size: $%.2f USDC per sisi", CONFIG["MAX_POSITION_SIZE"])
        log.info("=" * 50)

        balance = self.check_balance()
        log.info("Balance USDC: $%.2f", balance)

        while True:
            try:
                if self.consecutive_losses >= CONFIG["MAX_CONSECUTIVE_LOSSES"]:
                    log.warning(
                        "Terlalu banyak consecutive losses (%d). Bot pause 5 menit...",
                        self.consecutive_losses
                    )
                    time.sleep(300)
                    self.consecutive_losses = 0
                    continue

                markets = self.fetch_markets()
                self.total_scanned += len(markets)

                for market in markets:
                    opp = self.check_arbitrage(market)
                    if opp:
                        self.total_opportunities += 1
                        log.info(
                            "PELUANG DITEMUKAN: %s | YES: %.4f NO: %.4f | Profit: %.2f%%",
                            opp.question[:60], opp.yes_ask, opp.no_ask, opp.profit_pct
                        )
                        self.execute_arbitrage(opp)

                self.print_stats()
                time.sleep(CONFIG["SCAN_INTERVAL"])

            except KeyboardInterrupt:
                log.info("Bot dihentikan oleh user.")
                self.print_stats()
                break
            except Exception as e:
                log.error("Error tidak terduga: %s. Retry dalam 30 detik...", e)
                time.sleep(30)


if __name__ == "__main__":
    bot = PolymarketArbBot()
    bot.run()
