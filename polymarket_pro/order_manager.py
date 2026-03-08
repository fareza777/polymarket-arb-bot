"""
polymarket_pro/order_manager.py — Order Execution Engine
=========================================================
Wraps py-clob-client with proper order lifecycle management,
batch support, fee calculation, slippage protection, and retry logic.
"""

from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Optional

import structlog
from py_clob_client.client import ClobClient
from py_clob_client.clob_types import (
    ApiCreds,
    OrderArgs,
    OrderType as ClobOrderType,
    MarketOrderArgs,
    OrderBookSummary,
)
from py_clob_client.order_builder.constants import BUY, SELL

from .config import (
    AppConfig,
    PolymarketConfig,
    MAX_BATCH_ORDERS,
    API_RETRY_MAX,
    API_RETRY_BACKOFF,
    MIN_ORDER_SIZE_USDC,
    taker_fee_at_price,
    taker_fee_amount,
)
from .models import (
    Order,
    OrderSide,
    OrderType,
    OrderStatus,
    StrategyType,
    PriceLevel,
    OrderBook,
    Trade,
    Market,
)

logger = structlog.get_logger(__name__)


# ─────────────────────────────────────────────
# Rate Limiter
# ─────────────────────────────────────────────

class RateLimiter:
    """Simple token-bucket rate limiter for API calls."""

    def __init__(self, calls_per_second: int = 10):
        self._rate = calls_per_second
        self._tokens = float(calls_per_second)
        self._max_tokens = float(calls_per_second)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(
                self._max_tokens,
                self._tokens + elapsed * self._rate,
            )
            self._last_refill = now

            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self._rate
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0


# ─────────────────────────────────────────────
# Order Manager
# ─────────────────────────────────────────────

class OrderManager:
    """
    Complete order execution engine.
    
    Responsibilities:
    - Initialize and manage ClobClient connection
    - Create, sign, and post orders (all types: GTC, GTD, FOK, FAK)
    - Batch order submission (up to 15)
    - Post-only order support
    - Order tracking and state management
    - Cancel orders (single, all, by market)
    - Order book queries with caching
    - Fee calculation and slippage protection
    - Rate limiting and retry logic
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self._poly = config.polymarket
        
        # Initialize ClobClient
        self._client: Optional[ClobClient] = None
        self._rate_limiter = RateLimiter(calls_per_second=config.polymarket.chain_id)
        
        # Order tracking
        self._orders: dict[str, Order] = {}           # order_id -> Order
        self._active_orders: dict[str, set[str]] = defaultdict(set)  # market -> {order_ids}
        self._lock = asyncio.Lock()
        
        # Stats
        self._total_orders_placed = 0
        self._total_orders_filled = 0
        self._total_orders_failed = 0
        self._total_fees_paid = 0.0

    # ── Initialization ──────────────────────────

    def initialize(self) -> None:
        """Initialize the CLOB client with API credentials."""
        creds = ApiCreds(
            api_key=self._poly.api_key,
            api_secret=self._poly.api_secret,
            api_passphrase=self._poly.passphrase,
        )
        
        self._client = ClobClient(
            host=self._poly.clob_url,
            chain_id=self._poly.chain_id,
            key=self._poly.private_key,
            creds=creds,
        )
        
        logger.info(
            "OrderManager initialized",
            host=self._poly.clob_url,
            chain_id=self._poly.chain_id,
        )

    @property
    def client(self) -> ClobClient:
        if self._client is None:
            raise RuntimeError("OrderManager not initialized. Call initialize() first.")
        return self._client

    # ── Order Creation ──────────────────────────

    async def place_order(
        self,
        token_id: str,
        side: OrderSide,
        price: float,
        size: float,
        order_type: OrderType = OrderType.GTC,
        tick_size: float = 0.01,
        neg_risk: bool = False,
        post_only: bool = False,
        expiration: Optional[int] = None,
        strategy: StrategyType = StrategyType.MANUAL,
        market_condition_id: str = "",
        outcome: str = "",
    ) -> Optional[Order]:
        """
        Create, sign, and post a single order.
        
        Args:
            token_id: Asset token ID (YES or NO)
            side: BUY or SELL
            price: Limit price (0.0 to 1.0)
            size: Order size in USDC (for BUY) or shares (for SELL)
            order_type: GTC, GTD, FOK, or FAK
            tick_size: Market tick size (0.001, 0.01, or 0.1)
            neg_risk: Whether market uses neg risk CTF exchange
            post_only: If True, reject if order would match immediately
            expiration: Unix timestamp for GTD orders
            strategy: Which strategy placed this order
            market_condition_id: Market condition ID for tracking
            outcome: "yes" or "no"
        
        Returns:
            Order object if successful, None if failed
        """
        # Validate
        if size < MIN_ORDER_SIZE_USDC:
            logger.warning("Order size below minimum", size=size, min=MIN_ORDER_SIZE_USDC)
            return None

        if price <= 0 or price >= 1:
            logger.warning("Invalid price", price=price)
            return None

        # Build order tracking object
        order = Order(
            market_condition_id=market_condition_id,
            token_id=token_id,
            outcome=outcome,
            side=side,
            price=price,
            size=size,
            order_type=order_type,
            post_only=post_only,
            expiration=expiration,
            strategy=strategy,
            tick_size=tick_size,
            neg_risk=neg_risk,
        )

        try:
            await self._rate_limiter.acquire()
            
            # Map to py-clob-client types
            clob_side = BUY if side == OrderSide.BUY else SELL
            
            # Build order args
            order_args = OrderArgs(
                token_id=token_id,
                price=price,
                size=size,
                side=clob_side,
            )

            # Build options
            options = {
                "tick_size": str(tick_size),
                "neg_risk": neg_risk,
            }

            # Map order type
            if order_type == OrderType.FOK:
                clob_type = ClobOrderType.FOK
            elif order_type == OrderType.FAK:
                clob_type = ClobOrderType.FAK
            elif order_type == OrderType.GTD:
                clob_type = ClobOrderType.GTD
                if expiration:
                    options["expiration"] = str(expiration)
            else:
                clob_type = ClobOrderType.GTC

            # Create signed order
            loop = asyncio.get_running_loop()
            signed_order = await loop.run_in_executor(
                None,
                lambda: self.client.create_order(order_args, options=options),
            )

            # Set post-only if requested (only GTC/GTD)
            if post_only and order_type in (OrderType.GTC, OrderType.GTD):
                signed_order["postOnly"] = True

            # Set order type
            signed_order["orderType"] = clob_type.value

            # Post to CLOB
            result = await loop.run_in_executor(
                None,
                lambda: self.client.post_order(signed_order, clob_type),
            )

            # Parse result
            if result and result.get("success"):
                order.order_id = result.get("orderID", "")
                order.status = OrderStatus.OPEN
                
                async with self._lock:
                    self._orders[order.order_id] = order
                    self._active_orders[market_condition_id].add(order.order_id)
                    self._total_orders_placed += 1
                
                logger.info(
                    "Order placed",
                    order_id=order.order_id,
                    side=side.value,
                    price=price,
                    size=size,
                    type=order_type.value,
                    strategy=strategy.value,
                )
                return order
            else:
                order.status = OrderStatus.FAILED
                self._total_orders_failed += 1
                error_msg = result.get("errorMsg", "Unknown error") if result else "No response"
                logger.error("Order failed", error=error_msg, price=price, size=size)
                return None

        except Exception as e:
            order.status = OrderStatus.FAILED
            self._total_orders_failed += 1
            logger.error("Order placement exception", error=str(e))
            return None

    # ── Batch Orders ────────────────────────────

    async def place_batch_orders(
        self,
        orders_spec: list[dict],
        tick_size: float = 0.01,
        neg_risk: bool = False,
    ) -> list[Optional[Order]]:
        """
        Place multiple orders in a single batch (max 15).
        
        Args:
            orders_spec: List of dicts with keys:
                token_id, side (OrderSide), price, size, order_type (OrderType)
            tick_size: Market tick size
            neg_risk: Whether market uses neg risk
        
        Returns:
            List of Order objects (None for failed ones)
        """
        if len(orders_spec) > MAX_BATCH_ORDERS:
            logger.warning(
                f"Batch size {len(orders_spec)} exceeds max {MAX_BATCH_ORDERS}. Truncating."
            )
            orders_spec = orders_spec[:MAX_BATCH_ORDERS]

        results: list[Optional[Order]] = []
        signed_orders = []
        order_objects = []

        options = {
            "tick_size": str(tick_size),
            "neg_risk": neg_risk,
        }

        loop = asyncio.get_running_loop()

        for spec in orders_spec:
            try:
                clob_side = BUY if spec["side"] == OrderSide.BUY else SELL
                order_args = OrderArgs(
                    token_id=spec["token_id"],
                    price=spec["price"],
                    size=spec["size"],
                    side=clob_side,
                )

                signed = await loop.run_in_executor(
                    None,
                    lambda args=order_args: self.client.create_order(args, options=options),
                )

                ot = spec.get("order_type", OrderType.GTC)
                if ot == OrderType.FOK:
                    signed["orderType"] = ClobOrderType.FOK.value
                elif ot == OrderType.FAK:
                    signed["orderType"] = ClobOrderType.FAK.value
                elif ot == OrderType.GTD:
                    signed["orderType"] = ClobOrderType.GTD.value
                else:
                    signed["orderType"] = ClobOrderType.GTC.value

                if spec.get("post_only") and ot in (OrderType.GTC, OrderType.GTD):
                    signed["postOnly"] = True

                signed_orders.append(signed)

                order = Order(
                    token_id=spec["token_id"],
                    side=spec["side"],
                    price=spec["price"],
                    size=spec["size"],
                    order_type=ot,
                    strategy=spec.get("strategy", StrategyType.MANUAL),
                    market_condition_id=spec.get("market_condition_id", ""),
                    outcome=spec.get("outcome", ""),
                    tick_size=tick_size,
                    neg_risk=neg_risk,
                )
                order_objects.append(order)

            except Exception as e:
                logger.error("Failed to create batch order", error=str(e))
                order_objects.append(None)
                signed_orders.append(None)

        # Filter out failed creates
        valid_signed = [s for s in signed_orders if s is not None]

        if not valid_signed:
            return [None] * len(orders_spec)

        try:
            await self._rate_limiter.acquire()
            batch_result = await loop.run_in_executor(
                None,
                lambda: self.client.post_orders(valid_signed),
            )

            # Parse results
            valid_idx = 0
            for i, (signed, order) in enumerate(zip(signed_orders, order_objects)):
                if signed is None or order is None:
                    results.append(None)
                    continue

                if batch_result and valid_idx < len(batch_result):
                    res = batch_result[valid_idx]
                    if res.get("success"):
                        order.order_id = res.get("orderID", "")
                        order.status = OrderStatus.OPEN
                        async with self._lock:
                            self._orders[order.order_id] = order
                            self._total_orders_placed += 1
                        results.append(order)
                    else:
                        order.status = OrderStatus.FAILED
                        self._total_orders_failed += 1
                        results.append(None)
                    valid_idx += 1
                else:
                    results.append(None)

        except Exception as e:
            logger.error("Batch post failed", error=str(e))
            results = [None] * len(orders_spec)

        logger.info(
            "Batch orders placed",
            total=len(orders_spec),
            success=sum(1 for r in results if r is not None),
        )
        return results

    # ── Cancel Orders ───────────────────────────

    async def cancel_order(self, order_id: str) -> bool:
        """Cancel a single order. Returns True if successful."""
        try:
            await self._rate_limiter.acquire()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.cancel(order_id),
            )

            if result and result.get("canceled"):
                async with self._lock:
                    if order_id in self._orders:
                        self._orders[order_id].status = OrderStatus.CANCELLED
                        # Remove from active
                        for market_orders in self._active_orders.values():
                            market_orders.discard(order_id)
                
                logger.info("Order cancelled", order_id=order_id)
                return True
            else:
                logger.warning("Cancel failed", order_id=order_id, result=result)
                return False

        except Exception as e:
            logger.error("Cancel exception", order_id=order_id, error=str(e))
            return False

    async def cancel_all(self) -> int:
        """Cancel all open orders. Returns count of cancelled orders."""
        try:
            await self._rate_limiter.acquire()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.cancel_all(),
            )

            cancelled = 0
            if result and result.get("canceled"):
                cancelled_ids = result["canceled"]
                async with self._lock:
                    for oid in cancelled_ids:
                        if oid in self._orders:
                            self._orders[oid].status = OrderStatus.CANCELLED
                    self._active_orders.clear()
                cancelled = len(cancelled_ids)

            logger.info("All orders cancelled", count=cancelled)
            return cancelled

        except Exception as e:
            logger.error("Cancel all exception", error=str(e))
            return 0

    async def cancel_market_orders(self, market_condition_id: str) -> int:
        """Cancel all orders for a specific market."""
        async with self._lock:
            order_ids = list(self._active_orders.get(market_condition_id, set()))

        if not order_ids:
            return 0

        cancelled = 0
        for oid in order_ids:
            if await self.cancel_order(oid):
                cancelled += 1

        return cancelled

    # ── Order Book Queries ──────────────────────

    async def get_order_book(self, token_id: str) -> Optional[OrderBook]:
        """Fetch order book for a single token."""
        try:
            await self._rate_limiter.acquire()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.get_order_book(token_id),
            )

            if result is None:
                return None

            return self._parse_order_book(token_id, result)

        except Exception as e:
            logger.error("get_order_book failed", token_id=token_id, error=str(e))
            return None

    async def get_order_books(self, token_ids: list[str]) -> dict[str, OrderBook]:
        """Fetch order books for multiple tokens (batch)."""
        try:
            await self._rate_limiter.acquire()
            loop = asyncio.get_running_loop()
            results = await loop.run_in_executor(
                None,
                lambda: self.client.get_order_books(token_ids),
            )

            books = {}
            if results:
                for item in results:
                    # OrderBookSummary has .asset_id, .bids, .asks
                    asset_id = item.asset_id if hasattr(item, "asset_id") else item.get("asset_id", "")
                    books[asset_id] = self._parse_order_book(asset_id, item)

            return books

        except Exception as e:
            logger.error("get_order_books failed", error=str(e))
            return {}

    def _parse_order_book(self, asset_id: str, data) -> OrderBook:
        """Parse OrderBookSummary or dict into our OrderBook model."""
        bids_raw = data.bids if hasattr(data, "bids") else data.get("bids", [])
        asks_raw = data.asks if hasattr(data, "asks") else data.get("asks", [])

        bids = []
        for b in bids_raw:
            price = float(b.price if hasattr(b, "price") else b.get("price", 0))
            size = float(b.size if hasattr(b, "size") else b.get("size", 0))
            bids.append(PriceLevel(price=price, size=size))

        asks = []
        for a in asks_raw:
            price = float(a.price if hasattr(a, "price") else a.get("price", 0))
            size = float(a.size if hasattr(a, "size") else a.get("size", 0))
            asks.append(PriceLevel(price=price, size=size))

        # Sort: bids descending, asks ascending
        bids.sort(key=lambda x: x.price, reverse=True)
        asks.sort(key=lambda x: x.price)

        return OrderBook(asset_id=asset_id, bids=bids, asks=asks)

    # ── Price Helpers ───────────────────────────

    async def get_price(self, token_id: str) -> Optional[float]:
        """Get best available price for a token."""
        try:
            await self._rate_limiter.acquire()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.get_price(token_id),
            )
            if result is not None:
                return float(result)
            return None
        except Exception as e:
            logger.error("get_price failed", token_id=token_id, error=str(e))
            return None

    async def get_midpoint(self, token_id: str) -> Optional[float]:
        """Get midpoint price."""
        try:
            await self._rate_limiter.acquire()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.get_midpoint(token_id),
            )
            if result is not None:
                return float(result)
            return None
        except Exception as e:
            logger.error("get_midpoint failed", error=str(e))
            return None

    async def get_spread(self, token_id: str) -> Optional[float]:
        """Get current spread."""
        try:
            await self._rate_limiter.acquire()
            loop = asyncio.get_running_loop()
            result = await loop.run_in_executor(
                None,
                lambda: self.client.get_spread(token_id),
            )
            if result is not None:
                return float(result)
            return None
        except Exception as e:
            logger.error("get_spread failed", error=str(e))
            return None

    # ── Slippage Protection ─────────────────────

    async def place_order_with_slippage_check(
        self,
        token_id: str,
        side: OrderSide,
        price: float,
        size: float,
        max_slippage: float = 0.005,
        **kwargs,
    ) -> Optional[Order]:
        """
        Place order only if slippage is within threshold.
        
        Fetches current order book, calculates VWAP for target size,
        and rejects if slippage exceeds max_slippage.
        """
        book = await self.get_order_book(token_id)
        if book is None:
            logger.warning("Cannot check slippage — no order book")
            return await self.place_order(token_id, side, price, size, **kwargs)

        slippage = book.slippage(side, size)
        if slippage is not None and slippage > max_slippage:
            logger.warning(
                "Slippage too high — order rejected",
                slippage=f"{slippage:.4f}",
                max=f"{max_slippage:.4f}",
                token_id=token_id,
            )
            return None

        return await self.place_order(token_id, side, price, size, **kwargs)

    # ── Fee Helpers ─────────────────────────────

    @staticmethod
    def estimate_fee(price: float, size: float, market_type: str = "crypto") -> float:
        """Estimate taker fee for an order."""
        return taker_fee_amount(price, size, market_type)

    @staticmethod
    def fee_rate(price: float, market_type: str = "crypto") -> float:
        """Get fee rate at a given price."""
        return taker_fee_at_price(price, market_type)

    # ── Order State Management ──────────────────

    def get_order(self, order_id: str) -> Optional[Order]:
        """Get tracked order by ID."""
        return self._orders.get(order_id)

    def get_active_orders(self, market_condition_id: Optional[str] = None) -> list[Order]:
        """Get all active (non-terminal) orders, optionally filtered by market."""
        orders = []
        for oid, order in self._orders.items():
            if order.is_active:
                if market_condition_id is None or order.market_condition_id == market_condition_id:
                    orders.append(order)
        return orders

    async def process_fill(self, order_id: str, fill_price: float, fill_size: float) -> Optional[Trade]:
        """
        Process an order fill (from WS user channel).
        Updates order state and returns a Trade record.
        """
        async with self._lock:
            order = self._orders.get(order_id)
            if order is None:
                logger.warning("Fill for unknown order", order_id=order_id)
                return None

            order.update_fill(fill_price, fill_size)
            
            fee = taker_fee_amount(fill_price, fill_size)
            self._total_fees_paid += fee

            if order.is_terminal:
                self._total_orders_filled += 1
                for market_orders in self._active_orders.values():
                    market_orders.discard(order_id)

            trade = Trade(
                order_id=order_id,
                market_condition_id=order.market_condition_id,
                token_id=order.token_id,
                outcome=order.outcome,
                side=order.side,
                price=fill_price,
                size=fill_size,
                fee=fee,
                strategy=order.strategy,
                order_type=order.order_type,
            )

            logger.info(
                "Fill processed",
                order_id=order_id,
                price=fill_price,
                size=fill_size,
                fee=fee,
                remaining=order.remaining_size,
            )
            return trade

    # ── Retry Logic ─────────────────────────────

    async def place_order_with_retry(
        self,
        max_retries: int = API_RETRY_MAX,
        **kwargs,
    ) -> Optional[Order]:
        """Place order with automatic retry on transient failures."""
        for attempt in range(max_retries + 1):
            result = await self.place_order(**kwargs)
            if result is not None:
                return result

            if attempt < max_retries:
                delay = API_RETRY_BACKOFF * (2 ** attempt)
                logger.info(
                    "Retrying order",
                    attempt=attempt + 1,
                    max=max_retries,
                    delay=delay,
                )
                await asyncio.sleep(delay)

        logger.error("Order failed after all retries", retries=max_retries)
        return None

    # ── Stats ───────────────────────────────────

    def stats(self) -> dict:
        active = sum(1 for o in self._orders.values() if o.is_active)
        return {
            "total_placed": self._total_orders_placed,
            "total_filled": self._total_orders_filled,
            "total_failed": self._total_orders_failed,
            "active_orders": active,
            "total_fees": round(self._total_fees_paid, 4),
            "tracked_orders": len(self._orders),
        }

    # ── Cleanup ─────────────────────────────────

    async def cleanup_old_orders(self, max_age_hours: float = 24.0) -> int:
        """Remove terminal orders older than max_age_hours from tracking."""
        cutoff = time.time() - (max_age_hours * 3600)
        removed = 0

        async with self._lock:
            to_remove = [
                oid for oid, order in self._orders.items()
                if order.is_terminal and order.updated_at < cutoff
            ]
            for oid in to_remove:
                del self._orders[oid]
                removed += 1

        if removed:
            logger.info("Cleaned up old orders", removed=removed)
        return removed
