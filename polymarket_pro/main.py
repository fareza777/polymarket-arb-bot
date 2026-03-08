"""
polymarket_pro/main.py — Application Launcher
===============================================
Entry point for the Polymarket Pro trading bot.
Handles initialization, market discovery, and graceful shutdown.

Usage:
    python -m polymarket_pro.main                     # Live trading
    python -m polymarket_pro.main --dry-run            # Paper trading
    python -m polymarket_pro.main --dashboard           # Launch with dashboard
    python -m polymarket_pro.main --strategies arb      # Arb only
    python -m polymarket_pro.main --strategies mm       # MM only
    python -m polymarket_pro.main --strategies arb,mm   # Both (default)
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import subprocess
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional

import aiohttp
import structlog

from .config import (
    AppConfig,
    GAMMA_API_URL,
    load_config_from_env,
)
from .models import Market, MarketStatus, BotState
from .websocket_manager import WebSocketManager
from .order_manager import OrderManager
from .strategies import StrategyManager
from .risk_manager import RiskManager

logger = structlog.get_logger(__name__)


# ─────────────────────────────────────────────
# Logging Setup
# ─────────────────────────────────────────────

def setup_logging(config: AppConfig) -> None:
    """Configure structured logging with file rotation and console output."""
    import logging

    log_dir = Path(config.log.log_dir)
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / config.log.log_file

    # Root logger
    root = logging.getLogger()
    root.setLevel(getattr(logging, config.log.level))

    # File handler with rotation
    file_handler = RotatingFileHandler(
        log_path,
        maxBytes=config.log.max_bytes,
        backupCount=config.log.backup_count,
    )
    file_handler.setLevel(getattr(logging, config.log.level))

    # Console handler
    if config.log.console_output:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(getattr(logging, config.log.level))
        root.addHandler(console_handler)

    root.addHandler(file_handler)

    # Structlog configuration
    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            structlog.processors.add_log_level,
            structlog.processors.StackInfoRenderer(),
            structlog.dev.set_exc_info,
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.dev.ConsoleRenderer()
            if not config.log.json_format
            else structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            getattr(logging, config.log.level)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )


# ─────────────────────────────────────────────
# Market Discovery
# ─────────────────────────────────────────────

async def discover_markets(config: AppConfig) -> dict[str, Market]:
    """
    Fetch active binary markets from Polymarket Gamma API.
    Returns dict of condition_id -> Market.
    """
    markets = {}
    url = f"{config.polymarket.gamma_url}/markets"
    params = {
        "closed": "false",
        "limit": 100,
        "order": "volume24hr",
        "ascending": "false",
    }

    logger.info("Discovering markets from Gamma API")

    try:
        async with aiohttp.ClientSession() as session:
            next_cursor = None
            page = 0

            while True:
                if next_cursor:
                    params["cursor"] = next_cursor

                async with session.get(url, params=params) as resp:
                    if resp.status != 200:
                        logger.error("Gamma API error", status=resp.status)
                        break

                    data = await resp.json()

                    if isinstance(data, list):
                        items = data
                        next_cursor = None
                    else:
                        items = data.get("data", data.get("markets", []))
                        next_cursor = data.get("next_cursor")

                    if not items:
                        break

                    for item in items:
                        try:
                            market = _parse_market(item)
                            if market:
                                markets[market.condition_id] = market
                        except Exception as e:
                            logger.debug("Failed to parse market", error=str(e))

                    page += 1
                    logger.info(f"Fetched page {page}", markets_so_far=len(markets))

                    if not next_cursor or page >= 10:
                        break

    except Exception as e:
        logger.error("Market discovery failed", error=str(e))

    logger.info(f"Discovered {len(markets)} active markets")
    return markets


def _parse_market(data: dict) -> Optional[Market]:
    """Parse a Gamma API market response into our Market model."""
    condition_id = data.get("conditionId", data.get("condition_id", ""))
    if not condition_id:
        return None

    # Get token IDs from outcomes/tokens
    tokens = data.get("clobTokenIds", data.get("tokens", []))
    if isinstance(tokens, str):
        tokens = json.loads(tokens) if tokens else []

    if len(tokens) < 2:
        # Try to get from nested structure
        outcomes_data = data.get("outcomes", [])
        if isinstance(outcomes_data, str):
            outcomes_data = json.loads(outcomes_data) if outcomes_data else []
        if len(outcomes_data) < 2:
            return None
        # For simple markets, first = Yes, second = No
        if isinstance(tokens, list) and len(tokens) >= 2:
            token_yes = str(tokens[0])
            token_no = str(tokens[1])
        else:
            return None
    else:
        token_yes = str(tokens[0])
        token_no = str(tokens[1])

    # Determine tick size
    min_tick = float(data.get("minimumTickSize", data.get("min_tick_size", 0.01)))
    if min_tick not in (0.001, 0.01, 0.1):
        min_tick = 0.01

    # Neg risk
    neg_risk = data.get("negRisk", data.get("neg_risk", False))
    if isinstance(neg_risk, str):
        neg_risk = neg_risk.lower() == "true"

    volume = float(data.get("volume24hr", data.get("volume_24h", 0)) or 0)
    liquidity = float(data.get("liquidity", 0) or 0)

    return Market(
        condition_id=condition_id,
        question=data.get("question", ""),
        slug=data.get("slug", data.get("market_slug", "")),
        token_id_yes=token_yes,
        token_id_no=token_no,
        tick_size=min_tick,
        neg_risk=neg_risk,
        volume_24h=volume,
        liquidity=liquidity,
        end_date=data.get("endDate", data.get("end_date_iso")),
    )


# ─────────────────────────────────────────────
# State Export Loop
# ─────────────────────────────────────────────

async def state_export_loop(
    risk_mgr: RiskManager,
    strategy_mgr: StrategyManager,
    ws_mgr: WebSocketManager,
    interval: float = 1.0,
) -> None:
    """Periodically export bot state to JSON for dashboard."""
    try:
        while True:
            try:
                strat_stats = strategy_mgr.stats()
                ws_status = ws_mgr.status()
                state = risk_mgr.export_state(strat_stats, ws_status)
                risk_mgr.write_state_file(state)
            except Exception as e:
                logger.error("State export error", error=str(e))

            await asyncio.sleep(interval)
    except asyncio.CancelledError:
        pass


# ─────────────────────────────────────────────
# Dashboard Launcher
# ─────────────────────────────────────────────

def launch_dashboard(config: AppConfig) -> Optional[subprocess.Popen]:
    """Launch Streamlit dashboard in a subprocess."""
    try:
        cmd = [
            sys.executable, "-m", "streamlit", "run",
            "polymarket_pro/dashboard.py",
            "--server.port", str(config.dashboard.port),
            "--server.address", config.dashboard.host,
            "--server.headless", "true",
            "--theme.base", "dark",
        ]
        proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        logger.info(
            "Dashboard launched",
            port=config.dashboard.port,
            pid=proc.pid,
        )
        return proc
    except Exception as e:
        logger.error("Failed to launch dashboard", error=str(e))
        return None


# ─────────────────────────────────────────────
# Main Application
# ─────────────────────────────────────────────

class PolymarketBot:
    """Main bot application orchestrator."""

    def __init__(self, config: AppConfig):
        self.config = config
        self.ws_mgr = WebSocketManager(config.polymarket)
        self.order_mgr = OrderManager(config)
        self.risk_mgr = RiskManager(config)
        self.strategy_mgr = StrategyManager(config, self.order_mgr, self.ws_mgr)

        self._markets: dict[str, Market] = {}
        self._running = False
        self._start_time = 0.0
        self._tasks: list[asyncio.Task] = []
        self._dashboard_proc: Optional[subprocess.Popen] = None

    async def start(
        self,
        strategies: list[str] | None = None,
        launch_dash: bool = False,
    ) -> None:
        """Start the bot."""
        self._running = True
        self._start_time = time.time()

        logger.info(
            "Starting Polymarket Pro",
            dry_run=self.config.dry_run,
            strategies=strategies,
        )

        # Validate config
        errors = self.config.validate()
        if errors:
            for err in errors:
                logger.error("Config error", error=err)
            if not self.config.dry_run:
                raise RuntimeError(f"Configuration errors: {errors}")
            logger.warning("Continuing in dry-run despite config errors")

        # Override strategy enables based on CLI
        if strategies:
            self.config.arb.enabled = "arb" in strategies
            self.config.mm.enabled = "mm" in strategies

        # Initialize order manager
        if not self.config.dry_run:
            self.order_mgr.initialize()
            logger.info("Order manager initialized")

        # Discover markets
        self._markets = await discover_markets(self.config)
        if not self._markets:
            logger.warning("No markets discovered — bot will wait for data")

        # Start WebSocket connections
        await self.ws_mgr.start()

        # Subscribe to market data
        all_asset_ids = []
        for market in self._markets.values():
            all_asset_ids.extend([market.token_id_yes, market.token_id_no])
        if all_asset_ids:
            await self.ws_mgr.subscribe_markets(all_asset_ids[:500])
            logger.info(f"Subscribed to {min(len(all_asset_ids), 500)} assets")

        # Start strategies
        await self.strategy_mgr.start(self._markets)

        # Start state export loop
        export_task = asyncio.create_task(
            state_export_loop(self.risk_mgr, self.strategy_mgr, self.ws_mgr)
        )
        self._tasks.append(export_task)

        # Launch dashboard
        if launch_dash and self.config.dashboard.enabled:
            self._dashboard_proc = launch_dashboard(self.config)

        logger.info(
            "Polymarket Pro running",
            markets=len(self._markets),
            arb=self.config.arb.enabled,
            mm=self.config.mm.enabled,
            dry_run=self.config.dry_run,
        )

        # Keep running
        try:
            while self._running:
                await asyncio.sleep(1)
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        """Graceful shutdown."""
        logger.info("Shutting down Polymarket Pro...")
        self._running = False

        # Stop strategies (cancels all orders)
        await self.strategy_mgr.stop()

        # Stop WebSocket
        await self.ws_mgr.stop()

        # Cancel background tasks
        for task in self._tasks:
            if not task.done():
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

        # Stop dashboard
        if self._dashboard_proc:
            self._dashboard_proc.terminate()
            logger.info("Dashboard stopped")

        # Final state export
        try:
            strat_stats = self.strategy_mgr.stats()
            ws_status = self.ws_mgr.status()
            state = self.risk_mgr.export_state(strat_stats, ws_status)
            self.risk_mgr.write_state_file(state)
        except Exception:
            pass

        uptime = time.time() - self._start_time
        logger.info(
            "Polymarket Pro stopped",
            uptime_seconds=round(uptime, 1),
            total_trades=self.risk_mgr.stats().get("total_trades", 0),
            final_pnl=self.risk_mgr.stats().get("realized_pnl", 0),
        )


# ─────────────────────────────────────────────
# CLI Entry Point
# ─────────────────────────────────────────────

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Polymarket Pro — Arbitrage + Market Making Bot",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python -m polymarket_pro.main                  # Live, both strategies
  python -m polymarket_pro.main --dry-run        # Paper trading
  python -m polymarket_pro.main --strategies arb  # Arbitrage only
  python -m polymarket_pro.main --dashboard       # With Streamlit dashboard
  python -m polymarket_pro.main --profile conservative --capital 500
        """,
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Paper trading mode (no real orders)",
    )
    parser.add_argument(
        "--strategies",
        type=str,
        default="arb,mm",
        help="Comma-separated strategies: arb, mm (default: arb,mm)",
    )
    parser.add_argument(
        "--dashboard",
        action="store_true",
        help="Launch Streamlit dashboard",
    )
    parser.add_argument(
        "--profile",
        type=str,
        choices=["conservative", "moderate", "aggressive"],
        help="Risk profile preset",
    )
    parser.add_argument(
        "--capital",
        type=float,
        help="Total USDC capital to allocate",
    )
    parser.add_argument(
        "--log-level",
        type=str,
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
        help="Logging level",
    )

    return parser.parse_args()


def main() -> None:
    """Main entry point."""
    args = parse_args()

    # Apply CLI args to environment for config loading
    if args.dry_run:
        os.environ["POLY_DRY_RUN"] = "true"
    if args.profile:
        os.environ["POLY_PROFILE"] = args.profile
    if args.capital:
        os.environ["POLY_CAPITAL"] = str(args.capital)
    if args.log_level:
        os.environ["POLY_LOG_LEVEL"] = args.log_level

    # Load config
    config = load_config_from_env()
    config.dry_run = config.dry_run or args.dry_run

    # Setup logging
    setup_logging(config)

    logger.info(
        "Polymarket Pro v2.0",
        dry_run=config.dry_run,
        capital=config.risk.total_capital,
        strategies=args.strategies,
    )

    # Parse strategies
    strategies = [s.strip() for s in args.strategies.split(",")]

    # Create bot
    bot = PolymarketBot(config)

    # Setup signal handlers
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)

    def signal_handler(sig, frame):
        logger.info(f"Received signal {sig}")
        loop.create_task(bot.stop())

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # Run
    try:
        loop.run_until_complete(
            bot.start(strategies=strategies, launch_dash=args.dashboard)
        )
    except KeyboardInterrupt:
        loop.run_until_complete(bot.stop())
    finally:
        loop.close()


if __name__ == "__main__":
    main()
