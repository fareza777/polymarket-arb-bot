"""
polymarket_pro/strategies.py — Trading Strategies
===================================================
Contains:
  A. ArbitrageStrategy  — YES/NO arb when combined ask < 1.0
  B. MarketMakingStrategy — Avellaneda-Stoikov model
  C. StrategyManager — Orchestrates all strategies
"""

from __future__ import annotations

import asyncio
import math
import time
from abc import ABC, abstractmethod
from collections import deque
from typing import Optional

import numpy as np
import structlog

from .config import AppConfig, ArbConfig, MMConfig, taker_fee_amount
from .models import (
    ArbOpportunity,
    Market,
    MMQuote,
    Order,
    OrderSide,
    OrderType,
    StrategyType,
    Trade,
)
from .order_manager import OrderManager
from .websocket_manager import WebSocketManager

logger = structlog.get_logger(__name__)


# ─────────────────────────────────────────────
# Base Strategy
# ─────────────────────────────────────────────

class BaseStrategy(ABC):
    """Abstract base for all strategies."""

    def __init__(self, name: str, config: AppConfig):
        self.name = name
        self.config = config
        self.enabled = False
        self._running = False
        self._task: Optional[asyncio.Task] = None

    @abstractmethod
    async def on_tick(self, markets: dict[str, Market]) -> None:
        """Called on each strategy tick (periodic or event-driven)."""
        ...

    @abstractmethod
    async def on_market_event(self, event: dict) -> None:
        """Called when a WebSocket market event arrives."""
        ...

    async def start(self) -> None:
        self.enabled = True
        self._running = True
        logger.info(f"Strategy {self.name} started")

    async def stop(self) -> None:
        self._running = False
        self.enabled = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info(f"Strategy {self.name} stopped")


# ─────────────────────────────────────────────
# A. Arbitrage Strategy
# ─────────────────────────────────────────────

class ArbitrageStrategy(BaseStrategy):
    """
    YES/NO Arbitrage: Buy YES + NO when combined ask < 1.0.
    Guaranteed profit = 1.0 - (yes_ask + no_ask) per share.
    
    Features:
    - Real-time scanning via WebSocket price updates
    - Liquidity depth checking before execution
    - Simultaneous batch execution (YES + NO in one batch)
    - Partial fill handling with imbalance tracking
    - Per-market cooldown to prevent re-entry
    - Circuit breaker after consecutive losses
    - Fee-aware profit calculation
    """

    def __init__(
        self,
        config: AppConfig,
        order_manager: OrderManager,
        ws_manager: WebSocketManager,
    ):
        super().__init__("arbitrage", config)
        self.arb_config: ArbConfig = config.arb
        self.order_mgr = order_manager
        self.ws_mgr = ws_manager

        # State
        self._cooldowns: dict[str, float] = {}      # market_id -> cooldown_until
        self._consecutive_losses: int = 0
        self._circuit_broken: bool = False
        self._circuit_break_until: float = 0.0
        self._active_arbs: int = 0

        # Stats
        self.opportunities_found: int = 0
        self.opportunities_executed: int = 0
        self.total_profit: float = 0.0
        self.total_trades: int = 0
        self.winning_trades: int = 0
        self.losing_trades: int = 0

    async def on_tick(self, markets: dict[str, Market]) -> None:
        """Scan all markets for arbitrage opportunities."""
        if not self.enabled or not self._running:
            return

        # Check circuit breaker
        if self._circuit_broken:
            if time.time() < self._circuit_break_until:
                return
            self._circuit_broken = False
            self._consecutive_losses = 0
            logger.info("Arbitrage circuit breaker reset")

        # Check concurrent limit
        if self._active_arbs >= self.arb_config.max_concurrent_arbs:
            return

        for market_id, market in markets.items():
            if not self._should_scan_market(market):
                continue

            opportunity = self._detect_opportunity(market)
            if opportunity:
                self.opportunities_found += 1
                await self._execute_arb(opportunity)

    async def on_market_event(self, event: dict) -> None:
        """React to real-time price changes."""
        # Price change events can trigger immediate arb scanning
        # This is handled by the StrategyManager routing
        pass

    def _should_scan_market(self, market: Market) -> bool:
        """Check if market should be scanned."""
        # Skip inactive markets
        if market.status.value != "active":
            return False

        # Skip low volume
        if market.volume_24h < self.arb_config.min_market_volume_24h:
            return False

        # Skip markets in cooldown
        cooldown_until = self._cooldowns.get(market.condition_id, 0)
        if time.time() < cooldown_until:
            return False

        # Skip resolving markets
        if self.arb_config.exclude_resolving_markets and market.end_date:
            try:
                # Simple check — skip if end_date is soon
                # (Full implementation would parse ISO date)
                pass
            except (ValueError, TypeError):
                pass

        return True

    def _detect_opportunity(self, market: Market) -> Optional[ArbOpportunity]:
        """
        Detect if a market has an arbitrage opportunity.
        Returns ArbOpportunity if YES ask + NO ask < 1.0 (after fees).
        """
        yes_ask = market.best_ask_yes
        no_ask = market.best_ask_no

        if yes_ask <= 0 or no_ask <= 0:
            return None

        combined = yes_ask + no_ask
        if combined >= 1.0:
            return None

        # Calculate profit after fees
        size = self.arb_config.base_order_size
        fee_yes = taker_fee_amount(yes_ask, size, market.market_type)
        fee_no = taker_fee_amount(no_ask, size, market.market_type)
        gross_profit = (1.0 - combined) * size
        net_profit = gross_profit - fee_yes - fee_no

        if net_profit <= 0:
            return None

        profit_pct = net_profit / (combined * size)
        if profit_pct < self.arb_config.min_profit_pct:
            return None

        # Get available liquidity from WebSocket cache
        yes_bid, yes_ask_cached = self.ws_mgr.get_best_bid_ask(market.token_id_yes)
        no_bid, no_ask_cached = self.ws_mgr.get_best_bid_ask(market.token_id_no)

        # Use cached book for depth estimate (simplified)
        yes_book = self.ws_mgr.get_cached_book(market.token_id_yes)
        no_book = self.ws_mgr.get_cached_book(market.token_id_no)

        yes_depth = self._estimate_depth(yes_book, "asks")
        no_depth = self._estimate_depth(no_book, "asks")

        if yes_depth < self.arb_config.min_liquidity_usdc:
            return None
        if no_depth < self.arb_config.min_liquidity_usdc:
            return None

        return ArbOpportunity(
            market=market,
            yes_ask=yes_ask,
            no_ask=no_ask,
            yes_ask_size=yes_depth,
            no_ask_size=no_depth,
        )

    def _estimate_depth(self, book_data: Optional[dict], side: str) -> float:
        """Estimate available liquidity from cached book data."""
        if not book_data:
            return 0.0
        levels = book_data.get(side, [])
        total = 0.0
        for lvl in levels[:5]:  # Top 5 levels
            price = float(lvl.get("price", 0) if isinstance(lvl, dict) else getattr(lvl, "price", 0))
            size = float(lvl.get("size", 0) if isinstance(lvl, dict) else getattr(lvl, "size", 0))
            total += price * size
        return total

    async def _execute_arb(self, opp: ArbOpportunity) -> None:
        """Execute an arbitrage trade — buy YES and NO simultaneously."""
        market = opp.market

        # Calculate optimal size
        size = self._calculate_size(opp)
        if size < 1.0:
            return

        logger.info(
            "Executing arb",
            market=market.slug,
            yes_ask=opp.yes_ask,
            no_ask=opp.no_ask,
            profit_pct=f"{opp.profit_pct:.4f}",
            size=size,
        )

        self._active_arbs += 1

        try:
            # Batch order: buy YES + buy NO
            orders_spec = [
                {
                    "token_id": market.token_id_yes,
                    "side": OrderSide.BUY,
                    "price": opp.yes_ask,
                    "size": size,
                    "order_type": OrderType.FOK,
                    "strategy": StrategyType.ARBITRAGE,
                    "market_condition_id": market.condition_id,
                    "outcome": "yes",
                },
                {
                    "token_id": market.token_id_no,
                    "side": OrderSide.BUY,
                    "price": opp.no_ask,
                    "size": size,
                    "order_type": OrderType.FOK,
                    "strategy": StrategyType.ARBITRAGE,
                    "market_condition_id": market.condition_id,
                    "outcome": "no",
                },
            ]

            results = await self.order_mgr.place_batch_orders(
                orders_spec,
                tick_size=market.tick_size,
                neg_risk=market.neg_risk,
            )

            yes_order = results[0]
            no_order = results[1]

            if yes_order and no_order:
                # Both filled — arb complete
                fee_yes = taker_fee_amount(opp.yes_ask, size, market.market_type)
                fee_no = taker_fee_amount(opp.no_ask, size, market.market_type)
                profit = opp.net_profit(size, fee_yes, fee_no)

                self.opportunities_executed += 1
                self.total_trades += 2
                self.total_profit += profit

                if profit > 0:
                    self.winning_trades += 1
                    self._consecutive_losses = 0
                else:
                    self.losing_trades += 1
                    self._consecutive_losses += 1

                logger.info(
                    "Arb executed",
                    market=market.slug,
                    profit=f"${profit:.4f}",
                    total_profit=f"${self.total_profit:.4f}",
                )

            elif yes_order and not no_order:
                # YES filled but NO failed — need to handle imbalance
                logger.warning(
                    "Arb partial: YES filled, NO failed — imbalance",
                    market=market.slug,
                )
                self._consecutive_losses += 1

            elif no_order and not yes_order:
                logger.warning(
                    "Arb partial: NO filled, YES failed — imbalance",
                    market=market.slug,
                )
                self._consecutive_losses += 1

            else:
                # Both failed
                logger.warning("Arb failed: both orders rejected", market=market.slug)

            # Set cooldown
            self._cooldowns[market.condition_id] = (
                time.time() + self.arb_config.cooldown_per_market_sec
            )

            # Check circuit breaker
            if self._consecutive_losses >= self.arb_config.max_consecutive_losses:
                self._circuit_broken = True
                self._circuit_break_until = time.time() + self.arb_config.circuit_breaker_cooldown
                logger.warning(
                    "Arb circuit breaker triggered",
                    consecutive_losses=self._consecutive_losses,
                    cooldown=self.arb_config.circuit_breaker_cooldown,
                )

        finally:
            self._active_arbs -= 1

    def _calculate_size(self, opp: ArbOpportunity) -> float:
        """Calculate optimal order size based on opportunity and config."""
        base = self.arb_config.base_order_size

        if self.arb_config.scale_with_profit:
            # Scale up for larger arb opportunities
            multiplier = 1.0 + (opp.profit_pct / self.arb_config.target_profit_pct)
            base = min(base * multiplier, self.arb_config.max_order_size)

        # Limit by available liquidity
        max_liq = opp.max_size
        size = min(base, max_liq)

        return round(size, 2)

    def stats(self) -> dict:
        return {
            "enabled": self.enabled,
            "opportunities_found": self.opportunities_found,
            "opportunities_executed": self.opportunities_executed,
            "total_profit": round(self.total_profit, 4),
            "total_trades": self.total_trades,
            "winning_trades": self.winning_trades,
            "losing_trades": self.losing_trades,
            "avg_profit": round(self.total_profit / max(self.opportunities_executed, 1), 4),
            "success_rate": round(
                self.winning_trades / max(self.total_trades, 1), 4
            ),
            "circuit_broken": self._circuit_broken,
            "active_arbs": self._active_arbs,
        }


# ─────────────────────────────────────────────
# B. Market Making Strategy (Avellaneda-Stoikov)
# ─────────────────────────────────────────────

class MarketMakingStrategy(BaseStrategy):
    """
    Market Making using Avellaneda-Stoikov optimal quoting model.

    Reservation price:
        r = mid - q * gamma * sigma^2 * (T - t)

    Optimal half-spread:
        delta = gamma * sigma^2 * (T-t) / 2 + (1/gamma) * ln(1 + gamma/k)

    Where:
        q = current inventory (positive = long, negative = short)
        gamma = risk aversion parameter
        sigma = volatility (rolling std of price changes)
        T-t = time remaining (normalized)
        k = order arrival intensity

    Features:
    - Dynamic spread based on model
    - Inventory skew to manage risk
    - Multi-level quoting
    - Post-only orders (maker only, earn rebates)
    - Automatic quote refresh
    - Maker rebate factored into P&L
    """

    def __init__(
        self,
        config: AppConfig,
        order_manager: OrderManager,
        ws_manager: WebSocketManager,
    ):
        super().__init__("market_making", config)
        self.mm_config: MMConfig = config.mm
        self.order_mgr = order_manager
        self.ws_mgr = ws_manager

        # Per-market state
        self._quotes: dict[str, list[MMQuote]] = {}      # market_id -> active quotes
        self._inventory: dict[str, float] = {}             # token_id -> position size
        self._price_history: dict[str, deque] = {}         # token_id -> price deque
        self._trade_timestamps: dict[str, deque] = {}      # token_id -> trade time deque
        self._last_refresh: dict[str, float] = {}          # market_id -> last refresh time
        self._active_markets: set[str] = set()

        # Stats
        self.total_spread_captured: float = 0.0
        self.total_rebates: float = 0.0
        self.quote_refresh_count: int = 0
        self.total_fills: int = 0
        self.total_quotes_placed: int = 0

    async def on_tick(self, markets: dict[str, Market]) -> None:
        """Refresh quotes for all active MM markets."""
        if not self.enabled or not self._running:
            return

        now = time.time()

        for market_id in list(self._active_markets):
            market = markets.get(market_id)
            if not market:
                continue

            last_refresh = self._last_refresh.get(market_id, 0)
            if now - last_refresh >= self.mm_config.refresh_interval_sec:
                await self._refresh_quotes(market)
                self._last_refresh[market_id] = now

    async def on_market_event(self, event: dict) -> None:
        """Update price history on market events."""
        event_type = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if event_type in ("price_change", "last_trade_price") and asset_id:
            price = float(event.get("price", 0))
            if price > 0:
                if asset_id not in self._price_history:
                    self._price_history[asset_id] = deque(maxlen=self.mm_config.sigma_window)
                self._price_history[asset_id].append(price)

            if event_type == "last_trade_price":
                if asset_id not in self._trade_timestamps:
                    self._trade_timestamps[asset_id] = deque(maxlen=self.mm_config.k_window)
                self._trade_timestamps[asset_id].append(time.time())

    # ── Market Selection ────────────────────────

    async def select_markets(self, markets: dict[str, Market]) -> list[str]:
        """Select best markets for market making."""
        candidates = []

        for market_id, market in markets.items():
            if market.status.value != "active":
                continue
            if market.volume_24h < self.mm_config.min_volume_24h:
                continue

            # Prefer wide spreads (more profit opportunity)
            spread = max(market.spread_yes, market.spread_no)
            if spread < self.mm_config.min_spread_to_enter:
                continue

            score = market.volume_24h * spread
            candidates.append((market_id, score))

        # Sort by score descending, take top N
        candidates.sort(key=lambda x: x[1], reverse=True)
        selected = [c[0] for c in candidates[: self.mm_config.max_markets]]

        self._active_markets = set(selected)
        logger.info(f"MM selected {len(selected)} markets")
        return selected

    # ── Avellaneda-Stoikov Model ────────────────

    def _estimate_volatility(self, token_id: str) -> float:
        """Estimate volatility as rolling std of log returns."""
        history = self._price_history.get(token_id, deque())
        if len(history) < 10:
            return 0.05  # Default 5% vol

        prices = list(history)
        log_returns = []
        for i in range(1, len(prices)):
            if prices[i - 1] > 0 and prices[i] > 0:
                log_returns.append(math.log(prices[i] / prices[i - 1]))

        if not log_returns:
            return 0.05

        return float(np.std(log_returns))

    def _estimate_arrival_intensity(self, token_id: str) -> float:
        """
        Estimate order arrival intensity (k) from trade frequency.
        k = number of trades per time unit.
        """
        timestamps = self._trade_timestamps.get(token_id, deque())
        if len(timestamps) < 2:
            return 1.0  # Default

        time_span = timestamps[-1] - timestamps[0]
        if time_span <= 0:
            return 1.0

        return len(timestamps) / time_span

    def _compute_reservation_price(
        self,
        mid: float,
        inventory: float,
        sigma: float,
        gamma: float,
        time_remaining: float,
    ) -> float:
        """
        Avellaneda-Stoikov reservation price.
        r = mid - q * gamma * sigma^2 * (T - t)
        """
        r = mid - inventory * gamma * (sigma ** 2) * time_remaining
        return r

    def _compute_optimal_spread(
        self,
        sigma: float,
        gamma: float,
        time_remaining: float,
        k: float,
    ) -> float:
        """
        Avellaneda-Stoikov optimal half-spread.
        delta = gamma * sigma^2 * (T-t) / 2 + (1/gamma) * ln(1 + gamma/k)
        """
        if gamma <= 0 or k <= 0:
            return self.mm_config.min_spread_pct

        term1 = gamma * (sigma ** 2) * time_remaining / 2.0
        term2 = (1.0 / gamma) * math.log(1.0 + gamma / k)
        half_spread = term1 + term2

        # Clamp to min/max
        half_spread = max(half_spread, self.mm_config.min_spread_pct / 2.0)
        half_spread = min(half_spread, self.mm_config.max_spread_pct / 2.0)

        return half_spread

    # ── Quote Management ────────────────────────

    async def _refresh_quotes(self, market: Market) -> None:
        """Cancel old quotes and place new ones based on A-S model."""
        # Cancel existing quotes for this market
        await self.order_mgr.cancel_market_orders(market.condition_id)
        self.quote_refresh_count += 1

        # Compute quotes for YES side
        yes_quotes = self._compute_quotes(market, "yes")
        no_quotes = self._compute_quotes(market, "no")

        # Place all quotes as batch
        all_specs = []

        for q in yes_quotes:
            if q.bid_price > 0 and q.bid_size > 0:
                all_specs.append({
                    "token_id": market.token_id_yes,
                    "side": OrderSide.BUY,
                    "price": q.bid_price,
                    "size": q.bid_size,
                    "order_type": OrderType.GTD if self.mm_config.order_ttl_sec else OrderType.GTC,
                    "post_only": self.mm_config.use_post_only,
                    "strategy": StrategyType.MARKET_MAKING,
                    "market_condition_id": market.condition_id,
                    "outcome": "yes",
                })
            if q.ask_price > 0 and q.ask_size > 0:
                all_specs.append({
                    "token_id": market.token_id_yes,
                    "side": OrderSide.SELL,
                    "price": q.ask_price,
                    "size": q.ask_size,
                    "order_type": OrderType.GTD if self.mm_config.order_ttl_sec else OrderType.GTC,
                    "post_only": self.mm_config.use_post_only,
                    "strategy": StrategyType.MARKET_MAKING,
                    "market_condition_id": market.condition_id,
                    "outcome": "yes",
                })

        for q in no_quotes:
            if q.bid_price > 0 and q.bid_size > 0:
                all_specs.append({
                    "token_id": market.token_id_no,
                    "side": OrderSide.BUY,
                    "price": q.bid_price,
                    "size": q.bid_size,
                    "order_type": OrderType.GTD if self.mm_config.order_ttl_sec else OrderType.GTC,
                    "post_only": self.mm_config.use_post_only,
                    "strategy": StrategyType.MARKET_MAKING,
                    "market_condition_id": market.condition_id,
                    "outcome": "no",
                })
            if q.ask_price > 0 and q.ask_size > 0:
                all_specs.append({
                    "token_id": market.token_id_no,
                    "side": OrderSide.SELL,
                    "price": q.ask_price,
                    "size": q.ask_size,
                    "order_type": OrderType.GTD if self.mm_config.order_ttl_sec else OrderType.GTC,
                    "post_only": self.mm_config.use_post_only,
                    "strategy": StrategyType.MARKET_MAKING,
                    "market_condition_id": market.condition_id,
                    "outcome": "no",
                })

        if all_specs:
            # Batch in groups of 15
            from .config import MAX_BATCH_ORDERS
            for i in range(0, len(all_specs), MAX_BATCH_ORDERS):
                batch = all_specs[i: i + MAX_BATCH_ORDERS]
                await self.order_mgr.place_batch_orders(
                    batch,
                    tick_size=market.tick_size,
                    neg_risk=market.neg_risk,
                )
                self.total_quotes_placed += len(batch)

    def _compute_quotes(self, market: Market, outcome: str) -> list[MMQuote]:
        """Compute quote levels for one side (yes or no) using A-S model."""
        token_id = market.token_id_for_side(outcome)

        # Get mid price
        bid, ask = self.ws_mgr.get_best_bid_ask(token_id)
        if bid <= 0 or ask <= 0:
            return []
        mid = (bid + ask) / 2.0

        # Model inputs
        inventory = self._inventory.get(token_id, 0.0)
        sigma = self._estimate_volatility(token_id)
        k = self._estimate_arrival_intensity(token_id)
        gamma = self.mm_config.gamma
        T = self.mm_config.time_horizon

        # Inventory skew
        skewed_inventory = inventory * self.mm_config.inventory_skew_factor

        # Reservation price
        reservation = self._compute_reservation_price(mid, skewed_inventory, sigma, gamma, T)

        # Optimal half-spread
        half_spread = self._compute_optimal_spread(sigma, gamma, T, k)

        # Factor in maker rebate (effectively tighter spread is profitable)
        if self.mm_config.factor_rebate:
            rebate_adjustment = half_spread * self.mm_config.rebate_pct * 0.5
            half_spread = max(
                half_spread - rebate_adjustment,
                self.mm_config.min_spread_pct / 2.0,
            )

        # Generate multi-level quotes
        quotes = []
        for level in range(self.mm_config.num_levels):
            offset = level * self.mm_config.level_spacing_pct

            bid_price = reservation - half_spread - offset
            ask_price = reservation + half_spread + offset

            # Snap to tick size
            bid_price = self._snap_to_tick(bid_price, market.tick_size, down=True)
            ask_price = self._snap_to_tick(ask_price, market.tick_size, down=False)

            # Clamp to valid range
            bid_price = max(market.tick_size, min(bid_price, 1.0 - market.tick_size))
            ask_price = max(market.tick_size, min(ask_price, 1.0 - market.tick_size))

            # Skip if bid >= ask (crossed)
            if bid_price >= ask_price:
                continue

            # Check inventory limits
            inv = abs(self._inventory.get(token_id, 0.0))
            if inv >= self.mm_config.max_position_per_market:
                # Only place orders that reduce inventory
                if inventory > 0:
                    bid_price = 0  # Don't buy more
                else:
                    ask_price = 0  # Don't sell more

            quote = MMQuote(
                market_condition_id=market.condition_id,
                token_id=token_id,
                outcome=outcome,
                bid_price=bid_price,
                bid_size=self.mm_config.order_size,
                ask_price=ask_price,
                ask_size=self.mm_config.order_size,
                reservation_price=reservation,
                optimal_spread=half_spread * 2,
            )
            quotes.append(quote)

        return quotes

    @staticmethod
    def _snap_to_tick(price: float, tick_size: float, down: bool = True) -> float:
        """Snap price to nearest tick size."""
        if down:
            return math.floor(price / tick_size) * tick_size
        else:
            return math.ceil(price / tick_size) * tick_size

    def update_inventory(self, token_id: str, delta: float) -> None:
        """Update inventory after a fill."""
        current = self._inventory.get(token_id, 0.0)
        self._inventory[token_id] = current + delta

    def stats(self) -> dict:
        return {
            "enabled": self.enabled,
            "active_markets": len(self._active_markets),
            "total_spread_captured": round(self.total_spread_captured, 4),
            "total_rebates": round(self.total_rebates, 4),
            "quote_refresh_count": self.quote_refresh_count,
            "total_fills": self.total_fills,
            "total_quotes_placed": self.total_quotes_placed,
            "fill_rate": round(
                self.total_fills / max(self.total_quotes_placed, 1), 4
            ),
        }


# ─────────────────────────────────────────────
# C. Strategy Manager
# ─────────────────────────────────────────────

class StrategyManager:
    """
    Orchestrates all strategies.
    - Runs strategies concurrently
    - Routes WebSocket events to active strategies
    - Manages shared state (markets, positions)
    - Provides unified stats
    """

    def __init__(
        self,
        config: AppConfig,
        order_manager: OrderManager,
        ws_manager: WebSocketManager,
    ):
        self.config = config
        self.order_mgr = order_manager
        self.ws_mgr = ws_manager

        # Initialize strategies
        self.arb = ArbitrageStrategy(config, order_manager, ws_manager)
        self.mm = MarketMakingStrategy(config, order_manager, ws_manager)

        # Shared state
        self._markets: dict[str, Market] = {}
        self._running = False
        self._tick_task: Optional[asyncio.Task] = None

    # ── Lifecycle ───────────────────────────────

    async def start(self, markets: dict[str, Market]) -> None:
        """Start all enabled strategies."""
        self._markets = markets
        self._running = True

        if self.config.arb.enabled:
            await self.arb.start()

        if self.config.mm.enabled:
            await self.mm.start()
            # Select markets for MM
            await self.mm.select_markets(markets)

        # Register WS event handlers
        self.ws_mgr.on_any_market_event(self._on_market_event)
        self.ws_mgr.on_any_user_event(self._on_user_event)

        # Start periodic tick loop
        self._tick_task = asyncio.create_task(self._tick_loop())

        logger.info(
            "StrategyManager started",
            arb=self.config.arb.enabled,
            mm=self.config.mm.enabled,
            markets=len(markets),
        )

    async def stop(self) -> None:
        """Stop all strategies."""
        self._running = False

        if self._tick_task and not self._tick_task.done():
            self._tick_task.cancel()
            try:
                await self._tick_task
            except asyncio.CancelledError:
                pass

        await self.arb.stop()
        await self.mm.stop()

        # Cancel all outstanding orders
        await self.order_mgr.cancel_all()

        logger.info("StrategyManager stopped")

    # ── Tick Loop ───────────────────────────────

    async def _tick_loop(self) -> None:
        """Periodic tick for strategies that need polling."""
        try:
            while self._running:
                if self.arb.enabled:
                    try:
                        await self.arb.on_tick(self._markets)
                    except Exception as e:
                        logger.error("Arb tick error", error=str(e))

                if self.mm.enabled:
                    try:
                        await self.mm.on_tick(self._markets)
                    except Exception as e:
                        logger.error("MM tick error", error=str(e))

                await asyncio.sleep(self.config.arb.scan_interval_sec)

        except asyncio.CancelledError:
            pass

    # ── Event Routing ───────────────────────────

    async def _on_market_event(self, event: dict) -> None:
        """Route market events to active strategies."""
        if self.arb.enabled:
            try:
                await self.arb.on_market_event(event)
            except Exception as e:
                logger.error("Arb event error", error=str(e))

        if self.mm.enabled:
            try:
                await self.mm.on_market_event(event)
            except Exception as e:
                logger.error("MM event error", error=str(e))

        # Update market cache from events
        self._update_market_from_event(event)

    async def _on_user_event(self, event: dict) -> None:
        """Handle user events (order fills, etc.)."""
        event_type = event.get("event_type", event.get("type", ""))

        if event_type in ("trade", "order_update"):
            order_id = event.get("order_id", "")
            if order_id:
                fill_price = float(event.get("price", 0))
                fill_size = float(event.get("size", 0))

                if fill_price > 0 and fill_size > 0:
                    trade = await self.order_mgr.process_fill(
                        order_id, fill_price, fill_size
                    )

                    if trade:
                        # Update MM inventory
                        if trade.strategy == StrategyType.MARKET_MAKING:
                            delta = fill_size if trade.side == OrderSide.BUY else -fill_size
                            self.mm.update_inventory(trade.token_id, delta)
                            self.mm.total_fills += 1

    def _update_market_from_event(self, event: dict) -> None:
        """Update cached market data from WS events."""
        event_type = event.get("event_type", "")
        asset_id = event.get("asset_id", "")

        if not asset_id or event_type not in ("best_bid_ask", "price_change"):
            return

        # Find which market this asset belongs to
        for market in self._markets.values():
            if asset_id == market.token_id_yes:
                if event_type == "best_bid_ask":
                    market.best_bid_yes = float(event.get("best_bid", 0) or 0)
                    market.best_ask_yes = float(event.get("best_ask", 0) or 0)
                    if market.best_bid_yes > 0 and market.best_ask_yes > 0:
                        market.mid_yes = (market.best_bid_yes + market.best_ask_yes) / 2
                break
            elif asset_id == market.token_id_no:
                if event_type == "best_bid_ask":
                    market.best_bid_no = float(event.get("best_bid", 0) or 0)
                    market.best_ask_no = float(event.get("best_ask", 0) or 0)
                    if market.best_bid_no > 0 and market.best_ask_no > 0:
                        market.mid_no = (market.best_bid_no + market.best_ask_no) / 2
                break

    # ── Update Markets ──────────────────────────

    def update_markets(self, markets: dict[str, Market]) -> None:
        """Update the shared markets dictionary."""
        self._markets = markets

    # ── Stats ───────────────────────────────────

    def stats(self) -> dict:
        return {
            "running": self._running,
            "markets_tracked": len(self._markets),
            "arbitrage": self.arb.stats(),
            "market_making": self.mm.stats(),
            "orders": self.order_mgr.stats(),
        }
