"""
polymarket_pro/models.py — Data Models & Enums
================================================
Type-safe data structures for the entire system.
Semua komponen lain import dari sini.
"""

from __future__ import annotations

import time
import uuid
from dataclasses import dataclass, field
from enum import Enum, auto
from typing import Optional


# ─────────────────────────────────────────────
# Enums
# ─────────────────────────────────────────────

class OrderSide(Enum):
    BUY = "BUY"
    SELL = "SELL"


class OrderType(Enum):
    GTC = "GTC"     # Good-Til-Cancelled — rests on book
    GTD = "GTD"     # Good-Til-Date — expires at timestamp
    FOK = "FOK"     # Fill-Or-Kill — all or nothing, immediate
    FAK = "FAK"     # Fill-And-Kill — partial fill ok, cancel rest


class OrderStatus(Enum):
    PENDING = "pending"
    OPEN = "open"
    PARTIALLY_FILLED = "partially_filled"
    FILLED = "filled"
    CANCELLED = "cancelled"
    FAILED = "failed"
    EXPIRED = "expired"


class StrategyType(Enum):
    ARBITRAGE = "arbitrage"
    MARKET_MAKING = "market_making"
    MANUAL = "manual"


class MarketStatus(Enum):
    ACTIVE = "active"
    CLOSED = "closed"
    RESOLVED = "resolved"
    PAUSED = "paused"


class ConnectionState(Enum):
    DISCONNECTED = "disconnected"
    CONNECTING = "connecting"
    CONNECTED = "connected"
    RECONNECTING = "reconnecting"
    CLOSING = "closing"


class WSEventType(Enum):
    """Polymarket WebSocket event types."""
    BOOK = "book"
    PRICE_CHANGE = "price_change"
    LAST_TRADE_PRICE = "last_trade_price"
    BEST_BID_ASK = "best_bid_ask"
    TICK_SIZE_CHANGE = "tick_size_change"
    MARKET_RESOLVED = "market_resolved"
    # User channel events
    ORDER_UPDATE = "order_update"
    TRADE = "trade"


# ─────────────────────────────────────────────
# Market & Order Book Models
# ─────────────────────────────────────────────

@dataclass
class Market:
    """Represents a Polymarket binary market."""
    condition_id: str
    question: str
    slug: str
    
    # Token IDs for YES and NO outcomes
    token_id_yes: str
    token_id_no: str
    
    # Market parameters
    tick_size: float = 0.01
    neg_risk: bool = False
    min_order_size: float = 1.0
    market_type: str = "crypto"     # "crypto" or "sports"
    
    # Market metadata
    status: MarketStatus = MarketStatus.ACTIVE
    volume_24h: float = 0.0
    liquidity: float = 0.0
    end_date: Optional[str] = None
    
    # Cached prices
    best_bid_yes: float = 0.0
    best_ask_yes: float = 0.0
    best_bid_no: float = 0.0
    best_ask_no: float = 0.0
    mid_yes: float = 0.0
    mid_no: float = 0.0
    
    @property
    def spread_yes(self) -> float:
        if self.best_ask_yes > 0 and self.best_bid_yes > 0:
            return self.best_ask_yes - self.best_bid_yes
        return 0.0
    
    @property
    def spread_no(self) -> float:
        if self.best_ask_no > 0 and self.best_bid_no > 0:
            return self.best_ask_no - self.best_bid_no
        return 0.0
    
    @property
    def combined_ask(self) -> float:
        """YES ask + NO ask. If < 1.0, arbitrage opportunity exists."""
        if self.best_ask_yes > 0 and self.best_ask_no > 0:
            return self.best_ask_yes + self.best_ask_no
        return 2.0  # No opportunity
    
    @property
    def arb_profit_pct(self) -> float:
        """Potential arbitrage profit percentage (before fees)."""
        combined = self.combined_ask
        if combined >= 1.0:
            return 0.0
        return (1.0 - combined) / combined
    
    def token_id_for_side(self, outcome: str) -> str:
        """Get token ID for 'yes' or 'no'."""
        if outcome.lower() == "yes":
            return self.token_id_yes
        return self.token_id_no


@dataclass
class PriceLevel:
    """Single price level in the order book."""
    price: float
    size: float         # Size in USDC (for BUY) or shares (for SELL)
    
    @property
    def notional(self) -> float:
        return self.price * self.size


@dataclass
class OrderBook:
    """Full order book for one token (YES or NO)."""
    asset_id: str
    bids: list[PriceLevel] = field(default_factory=list)
    asks: list[PriceLevel] = field(default_factory=list)
    timestamp: float = field(default_factory=time.time)
    
    @property
    def best_bid(self) -> Optional[PriceLevel]:
        return self.bids[0] if self.bids else None
    
    @property
    def best_ask(self) -> Optional[PriceLevel]:
        return self.asks[0] if self.asks else None
    
    @property
    def mid_price(self) -> float:
        bb = self.best_bid
        ba = self.best_ask
        if bb and ba:
            return (bb.price + ba.price) / 2.0
        if bb:
            return bb.price
        if ba:
            return ba.price
        return 0.0
    
    @property
    def spread(self) -> float:
        bb = self.best_bid
        ba = self.best_ask
        if bb and ba:
            return ba.price - bb.price
        return float("inf")
    
    @property
    def spread_pct(self) -> float:
        mid = self.mid_price
        if mid > 0:
            return self.spread / mid
        return float("inf")
    
    def depth_at_price(self, side: OrderSide, levels: int = 5) -> float:
        """Total liquidity in top N levels."""
        book = self.bids if side == OrderSide.BUY else self.asks
        return sum(lvl.size for lvl in book[:levels])
    
    def vwap(self, side: OrderSide, target_size: float) -> Optional[float]:
        """
        Volume-weighted average price to fill target_size.
        Returns None if insufficient liquidity.
        """
        book = self.asks if side == OrderSide.BUY else self.bids
        remaining = target_size
        total_cost = 0.0
        
        for lvl in book:
            fill = min(remaining, lvl.size)
            total_cost += fill * lvl.price
            remaining -= fill
            if remaining <= 0:
                return total_cost / target_size
        
        return None  # Insufficient liquidity
    
    def slippage(self, side: OrderSide, target_size: float) -> Optional[float]:
        """Calculate slippage for a given order size."""
        vwap_price = self.vwap(side, target_size)
        if vwap_price is None:
            return None
        ref = self.best_ask.price if side == OrderSide.BUY else self.best_bid.price
        if ref <= 0:
            return None
        return abs(vwap_price - ref) / ref


@dataclass
class Spread:
    """Bid-ask spread summary."""
    asset_id: str
    bid: float
    ask: float
    
    @property
    def absolute(self) -> float:
        return self.ask - self.bid
    
    @property
    def mid(self) -> float:
        return (self.bid + self.ask) / 2.0
    
    @property
    def relative(self) -> float:
        mid = self.mid
        return self.absolute / mid if mid > 0 else float("inf")


# ─────────────────────────────────────────────
# Position & Trade Models
# ─────────────────────────────────────────────

@dataclass
class Position:
    """Tracks a position in a specific token."""
    market_condition_id: str
    token_id: str
    outcome: str            # "yes" or "no"
    strategy: StrategyType
    
    side: OrderSide = OrderSide.BUY
    size: float = 0.0               # Number of shares
    avg_entry_price: float = 0.0
    current_price: float = 0.0
    
    realized_pnl: float = 0.0
    total_fees_paid: float = 0.0
    
    opened_at: float = field(default_factory=time.time)
    last_updated: float = field(default_factory=time.time)
    
    @property
    def notional_value(self) -> float:
        """Current notional value of position."""
        return self.size * self.current_price
    
    @property
    def cost_basis(self) -> float:
        """Total cost to acquire position."""
        return self.size * self.avg_entry_price
    
    @property
    def unrealized_pnl(self) -> float:
        """Unrealized P&L at current price."""
        if self.side == OrderSide.BUY:
            return (self.current_price - self.avg_entry_price) * self.size
        else:
            return (self.avg_entry_price - self.current_price) * self.size
    
    @property
    def total_pnl(self) -> float:
        return self.realized_pnl + self.unrealized_pnl - self.total_fees_paid
    
    @property
    def pnl_pct(self) -> float:
        basis = self.cost_basis
        if basis > 0:
            return self.total_pnl / basis
        return 0.0
    
    def update_price(self, price: float) -> None:
        """Update current market price."""
        self.current_price = price
        self.last_updated = time.time()
    
    def add_fill(self, fill_price: float, fill_size: float, fee: float) -> None:
        """Process a new fill for this position."""
        if self.size == 0:
            self.avg_entry_price = fill_price
        else:
            # Weighted average
            total_cost = (self.avg_entry_price * self.size) + (fill_price * fill_size)
            self.avg_entry_price = total_cost / (self.size + fill_size)
        
        self.size += fill_size
        self.total_fees_paid += fee
        self.last_updated = time.time()
    
    def reduce(self, fill_price: float, fill_size: float, fee: float) -> float:
        """
        Reduce position. Returns realized P&L from this reduction.
        """
        fill_size = min(fill_size, self.size)
        if self.side == OrderSide.BUY:
            pnl = (fill_price - self.avg_entry_price) * fill_size
        else:
            pnl = (self.avg_entry_price - fill_price) * fill_size
        
        pnl -= fee
        self.realized_pnl += pnl
        self.size -= fill_size
        self.total_fees_paid += fee
        self.last_updated = time.time()
        return pnl


@dataclass
class Trade:
    """Record of an executed trade."""
    trade_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    order_id: str = ""
    market_condition_id: str = ""
    token_id: str = ""
    outcome: str = ""           # "yes" or "no"
    
    side: OrderSide = OrderSide.BUY
    price: float = 0.0
    size: float = 0.0
    fee: float = 0.0
    
    strategy: StrategyType = StrategyType.MANUAL
    order_type: OrderType = OrderType.GTC
    
    timestamp: float = field(default_factory=time.time)
    
    @property
    def notional(self) -> float:
        return self.price * self.size
    
    @property
    def net_cost(self) -> float:
        """Cost including fees."""
        return self.notional + self.fee
    
    def to_dict(self) -> dict:
        return {
            "trade_id": self.trade_id,
            "order_id": self.order_id,
            "market": self.market_condition_id,
            "token_id": self.token_id,
            "outcome": self.outcome,
            "side": self.side.value,
            "price": self.price,
            "size": self.size,
            "fee": self.fee,
            "strategy": self.strategy.value,
            "order_type": self.order_type.value,
            "timestamp": self.timestamp,
        }


# ─────────────────────────────────────────────
# Order Model
# ─────────────────────────────────────────────

@dataclass
class Order:
    """Represents an order in the system."""
    order_id: str = ""
    market_condition_id: str = ""
    token_id: str = ""
    outcome: str = ""
    
    side: OrderSide = OrderSide.BUY
    price: float = 0.0
    size: float = 0.0
    
    order_type: OrderType = OrderType.GTC
    post_only: bool = False
    expiration: Optional[int] = None    # Unix timestamp for GTD
    
    status: OrderStatus = OrderStatus.PENDING
    filled_size: float = 0.0
    avg_fill_price: float = 0.0
    
    strategy: StrategyType = StrategyType.MANUAL
    created_at: float = field(default_factory=time.time)
    updated_at: float = field(default_factory=time.time)
    
    # Polymarket-specific
    tick_size: float = 0.01
    neg_risk: bool = False
    
    @property
    def remaining_size(self) -> float:
        return self.size - self.filled_size
    
    @property
    def is_active(self) -> bool:
        return self.status in (OrderStatus.PENDING, OrderStatus.OPEN, OrderStatus.PARTIALLY_FILLED)
    
    @property
    def is_terminal(self) -> bool:
        return self.status in (OrderStatus.FILLED, OrderStatus.CANCELLED, OrderStatus.FAILED, OrderStatus.EXPIRED)
    
    @property
    def fill_pct(self) -> float:
        if self.size > 0:
            return self.filled_size / self.size
        return 0.0
    
    def update_fill(self, fill_price: float, fill_size: float) -> None:
        """Update order with a fill."""
        total_filled_cost = (self.avg_fill_price * self.filled_size) + (fill_price * fill_size)
        self.filled_size += fill_size
        self.avg_fill_price = total_filled_cost / self.filled_size if self.filled_size > 0 else 0.0
        
        if self.filled_size >= self.size:
            self.status = OrderStatus.FILLED
        else:
            self.status = OrderStatus.PARTIALLY_FILLED
        
        self.updated_at = time.time()


# ─────────────────────────────────────────────
# Strategy-Specific Models
# ─────────────────────────────────────────────

@dataclass
class ArbOpportunity:
    """Detected arbitrage opportunity."""
    market: Market
    yes_ask: float
    no_ask: float
    yes_ask_size: float             # Available liquidity
    no_ask_size: float
    
    timestamp: float = field(default_factory=time.time)
    
    @property
    def combined_cost(self) -> float:
        return self.yes_ask + self.no_ask
    
    @property
    def profit_per_share(self) -> float:
        """Gross profit per share (before fees)."""
        return 1.0 - self.combined_cost
    
    @property
    def profit_pct(self) -> float:
        if self.combined_cost > 0:
            return self.profit_per_share / self.combined_cost
        return 0.0
    
    @property
    def max_size(self) -> float:
        """Max executable size limited by available liquidity on both sides."""
        return min(self.yes_ask_size, self.no_ask_size)
    
    def net_profit(self, size: float, fee_yes: float, fee_no: float) -> float:
        """Calculate net profit after fees for a given size."""
        gross = self.profit_per_share * size
        return gross - fee_yes - fee_no
    
    def is_profitable_after_fees(self, size: float, market_type: str = "crypto") -> bool:
        """Check if opportunity is profitable after Polymarket fees."""
        from .config import taker_fee_amount
        fee_yes = taker_fee_amount(self.yes_ask, size, market_type)
        fee_no = taker_fee_amount(self.no_ask, size, market_type)
        return self.net_profit(size, fee_yes, fee_no) > 0


@dataclass
class MMQuote:
    """Market making quote (bid + ask pair)."""
    market_condition_id: str
    token_id: str
    outcome: str            # "yes" or "no"
    
    bid_price: float = 0.0
    bid_size: float = 0.0
    ask_price: float = 0.0
    ask_size: float = 0.0
    
    reservation_price: float = 0.0  # Avellaneda-Stoikov reservation price
    optimal_spread: float = 0.0     # Optimal spread from model
    
    bid_order_id: Optional[str] = None
    ask_order_id: Optional[str] = None
    
    timestamp: float = field(default_factory=time.time)
    
    @property
    def spread(self) -> float:
        return self.ask_price - self.bid_price
    
    @property
    def mid(self) -> float:
        return (self.bid_price + self.ask_price) / 2.0
    
    @property
    def is_active(self) -> bool:
        return self.bid_order_id is not None or self.ask_order_id is not None


# ─────────────────────────────────────────────
# Portfolio Snapshot
# ─────────────────────────────────────────────

@dataclass
class PortfolioSnapshot:
    """Point-in-time portfolio state for dashboard/analytics."""
    timestamp: float = field(default_factory=time.time)
    
    # Value
    total_value: float = 0.0        # Cash + positions value
    cash_balance: float = 0.0
    positions_value: float = 0.0
    
    # P&L
    total_realized_pnl: float = 0.0
    total_unrealized_pnl: float = 0.0
    total_fees: float = 0.0
    
    # Risk metrics
    num_positions: int = 0
    total_exposure: float = 0.0
    max_single_exposure: float = 0.0
    
    # Performance
    peak_value: float = 0.0
    drawdown: float = 0.0          # Current drawdown from peak
    drawdown_pct: float = 0.0
    
    # Stats (rolling)
    sharpe_ratio: float = 0.0
    win_rate: float = 0.0
    total_trades: int = 0
    winning_trades: int = 0
    losing_trades: int = 0
    avg_win: float = 0.0
    avg_loss: float = 0.0
    profit_factor: float = 0.0     # Gross wins / gross losses
    
    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp,
            "total_value": self.total_value,
            "cash_balance": self.cash_balance,
            "positions_value": self.positions_value,
            "realized_pnl": self.total_realized_pnl,
            "unrealized_pnl": self.total_unrealized_pnl,
            "total_fees": self.total_fees,
            "num_positions": self.num_positions,
            "total_exposure": self.total_exposure,
            "drawdown": self.drawdown,
            "drawdown_pct": self.drawdown_pct,
            "sharpe_ratio": self.sharpe_ratio,
            "win_rate": self.win_rate,
            "total_trades": self.total_trades,
            "profit_factor": self.profit_factor,
        }


# ─────────────────────────────────────────────
# Bot State — Shared Between Bot & Dashboard
# ─────────────────────────────────────────────

@dataclass
class BotState:
    """
    Complete bot state snapshot.
    Written to JSON by the bot, read by the dashboard.
    """
    # Connection
    ws_market_connected: bool = False
    ws_user_connected: bool = False
    uptime_seconds: float = 0.0
    
    # Strategy status
    arb_enabled: bool = False
    mm_enabled: bool = False
    kill_switch_active: bool = False
    
    # Current data
    portfolio: PortfolioSnapshot = field(default_factory=PortfolioSnapshot)
    positions: list[dict] = field(default_factory=list)
    recent_trades: list[dict] = field(default_factory=list)
    active_orders: list[dict] = field(default_factory=list)
    
    # Arbitrage stats
    arb_opportunities_found: int = 0
    arb_opportunities_executed: int = 0
    arb_total_profit: float = 0.0
    arb_avg_profit_per_trade: float = 0.0
    arb_success_rate: float = 0.0
    
    # MM stats
    mm_active_markets: int = 0
    mm_total_spread_captured: float = 0.0
    mm_total_rebates: float = 0.0
    mm_quote_refresh_count: int = 0
    mm_fill_rate: float = 0.0
    
    # Errors
    recent_errors: list[str] = field(default_factory=list)
    error_count: int = 0
    
    last_updated: float = field(default_factory=time.time)
    
    def to_dict(self) -> dict:
        """Serialize to dict for JSON export."""
        return {
            "ws_market_connected": self.ws_market_connected,
            "ws_user_connected": self.ws_user_connected,
            "uptime_seconds": self.uptime_seconds,
            "arb_enabled": self.arb_enabled,
            "mm_enabled": self.mm_enabled,
            "kill_switch_active": self.kill_switch_active,
            "portfolio": self.portfolio.to_dict(),
            "positions": self.positions,
            "recent_trades": self.recent_trades,
            "active_orders": self.active_orders,
            "arb_opportunities_found": self.arb_opportunities_found,
            "arb_opportunities_executed": self.arb_opportunities_executed,
            "arb_total_profit": self.arb_total_profit,
            "arb_avg_profit_per_trade": self.arb_avg_profit_per_trade,
            "arb_success_rate": self.arb_success_rate,
            "mm_active_markets": self.mm_active_markets,
            "mm_total_spread_captured": self.mm_total_spread_captured,
            "mm_total_rebates": self.mm_total_rebates,
            "mm_quote_refresh_count": self.mm_quote_refresh_count,
            "mm_fill_rate": self.mm_fill_rate,
            "recent_errors": self.recent_errors[-20:],
            "error_count": self.error_count,
            "last_updated": self.last_updated,
        }
