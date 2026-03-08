"""
polymarket_pro/risk_manager.py — Risk Management & Portfolio
=============================================================
Central risk management: position tracking, P&L, drawdown monitoring,
kill switch, trade journal, and portfolio metrics.
"""

from __future__ import annotations

import asyncio
import json
import math
import sqlite3
import time
from collections import deque
from pathlib import Path
from typing import Optional

import numpy as np
import structlog

from .config import AppConfig, RiskConfig
from .models import (
    BotState,
    Market,
    Order,
    OrderSide,
    Position,
    PortfolioSnapshot,
    StrategyType,
    Trade,
)

logger = structlog.get_logger(__name__)


# ─────────────────────────────────────────────
# Trade Journal (SQLite)
# ─────────────────────────────────────────────

class TradeJournal:
    """Persistent trade journal using SQLite."""

    def __init__(self, db_path: str = "data/trades.db"):
        self.db_path = db_path
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    def _init_db(self) -> None:
        with sqlite3.connect(self.db_path) as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id TEXT PRIMARY KEY,
                    order_id TEXT,
                    market_condition_id TEXT,
                    token_id TEXT,
                    outcome TEXT,
                    side TEXT,
                    price REAL,
                    size REAL,
                    fee REAL,
                    strategy TEXT,
                    order_type TEXT,
                    timestamp REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS snapshots (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp REAL,
                    total_value REAL,
                    cash_balance REAL,
                    positions_value REAL,
                    realized_pnl REAL,
                    unrealized_pnl REAL,
                    total_fees REAL,
                    num_positions INTEGER,
                    drawdown REAL,
                    drawdown_pct REAL,
                    sharpe_ratio REAL,
                    win_rate REAL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_timestamp ON trades(timestamp)"
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_trades_strategy ON trades(strategy)"
            )

    def record_trade(self, trade: Trade) -> None:
        """Record a trade to the journal."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """INSERT OR REPLACE INTO trades
                    (trade_id, order_id, market_condition_id, token_id, outcome,
                     side, price, size, fee, strategy, order_type, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        trade.trade_id,
                        trade.order_id,
                        trade.market_condition_id,
                        trade.token_id,
                        trade.outcome,
                        trade.side.value,
                        trade.price,
                        trade.size,
                        trade.fee,
                        trade.strategy.value,
                        trade.order_type.value,
                        trade.timestamp,
                    ),
                )
        except Exception as e:
            logger.error("Failed to record trade", error=str(e))

    def record_snapshot(self, snapshot: PortfolioSnapshot) -> None:
        """Record a portfolio snapshot."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.execute(
                    """INSERT INTO snapshots
                    (timestamp, total_value, cash_balance, positions_value,
                     realized_pnl, unrealized_pnl, total_fees, num_positions,
                     drawdown, drawdown_pct, sharpe_ratio, win_rate)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                    (
                        snapshot.timestamp,
                        snapshot.total_value,
                        snapshot.cash_balance,
                        snapshot.positions_value,
                        snapshot.total_realized_pnl,
                        snapshot.total_unrealized_pnl,
                        snapshot.total_fees,
                        snapshot.num_positions,
                        snapshot.drawdown,
                        snapshot.drawdown_pct,
                        snapshot.sharpe_ratio,
                        snapshot.win_rate,
                    ),
                )
        except Exception as e:
            logger.error("Failed to record snapshot", error=str(e))

    def get_recent_trades(self, limit: int = 50) -> list[dict]:
        """Get recent trades."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT * FROM trades ORDER BY timestamp DESC LIMIT ?",
                    (limit,),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception:
            return []

    def get_snapshots(self, hours: float = 24.0) -> list[dict]:
        """Get snapshots from last N hours."""
        cutoff = time.time() - (hours * 3600)
        try:
            with sqlite3.connect(self.db_path) as conn:
                conn.row_factory = sqlite3.Row
                rows = conn.execute(
                    "SELECT * FROM snapshots WHERE timestamp > ? ORDER BY timestamp",
                    (cutoff,),
                ).fetchall()
                return [dict(r) for r in rows]
        except Exception:
            return []

    def get_strategy_stats(self, strategy: str) -> dict:
        """Get aggregate stats for a strategy."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                row = conn.execute(
                    """SELECT
                        COUNT(*) as total_trades,
                        SUM(size * price) as total_volume,
                        SUM(fee) as total_fees,
                        AVG(price) as avg_price,
                        MIN(timestamp) as first_trade,
                        MAX(timestamp) as last_trade
                    FROM trades WHERE strategy = ?""",
                    (strategy,),
                ).fetchone()
                if row:
                    return {
                        "total_trades": row[0],
                        "total_volume": round(row[1] or 0, 2),
                        "total_fees": round(row[2] or 0, 4),
                        "avg_price": round(row[3] or 0, 4),
                        "first_trade": row[4],
                        "last_trade": row[5],
                    }
        except Exception:
            pass
        return {}


# ─────────────────────────────────────────────
# Risk Manager
# ─────────────────────────────────────────────

class RiskManager:
    """
    Central risk management system.
    
    Responsibilities:
    - Track all positions across strategies
    - Real-time P&L (realized + unrealized)
    - Drawdown monitoring with kill switch
    - Position limit enforcement
    - Balance validation
    - Portfolio snapshots for dashboard
    - Event risk management near resolution
    """

    def __init__(self, config: AppConfig):
        self.config = config
        self.risk_config: RiskConfig = config.risk
        
        # Position tracking: (market_id, token_id) -> Position
        self._positions: dict[tuple[str, str], Position] = {}
        self._lock = asyncio.Lock()
        
        # P&L tracking
        self._peak_value: float = config.risk.total_capital
        self._current_value: float = config.risk.total_capital
        self._cash_balance: float = config.risk.total_capital
        self._total_realized_pnl: float = 0.0
        self._total_fees: float = 0.0
        
        # Returns history for Sharpe calculation
        self._returns_history: deque[float] = deque(maxlen=500)
        self._last_snapshot_value: float = config.risk.total_capital
        
        # Trade stats
        self._total_trades: int = 0
        self._winning_trades: int = 0
        self._losing_trades: int = 0
        self._gross_wins: float = 0.0
        self._gross_losses: float = 0.0
        
        # Kill switch
        self._kill_switch_active: bool = False
        self._kill_switch_reason: str = ""
        
        # Error tracking
        self._consecutive_errors: int = 0
        self._hourly_losses: deque[tuple[float, float]] = deque()  # (timestamp, loss)
        
        # Journal
        self.journal = TradeJournal(config.dashboard.trades_db)
        
        # State file for dashboard
        self._state_file = config.dashboard.state_file
        Path(self._state_file).parent.mkdir(parents=True, exist_ok=True)

    # ── Position Management ─────────────────────

    async def record_trade(self, trade: Trade) -> None:
        """Record a trade and update positions."""
        async with self._lock:
            key = (trade.market_condition_id, trade.token_id)
            
            if trade.side == OrderSide.BUY:
                # Opening or adding to position
                if key in self._positions:
                    self._positions[key].add_fill(trade.price, trade.size, trade.fee)
                else:
                    pos = Position(
                        market_condition_id=trade.market_condition_id,
                        token_id=trade.token_id,
                        outcome=trade.outcome,
                        strategy=trade.strategy,
                        side=OrderSide.BUY,
                        avg_entry_price=trade.price,
                        current_price=trade.price,
                    )
                    pos.add_fill(trade.price, trade.size, trade.fee)
                    self._positions[key] = pos
                
                self._cash_balance -= trade.net_cost
                
            elif trade.side == OrderSide.SELL:
                # Reducing position
                if key in self._positions:
                    pnl = self._positions[key].reduce(trade.price, trade.size, trade.fee)
                    self._total_realized_pnl += pnl
                    self._cash_balance += (trade.price * trade.size) - trade.fee
                    
                    # Track win/loss
                    if pnl > 0:
                        self._winning_trades += 1
                        self._gross_wins += pnl
                    elif pnl < 0:
                        self._losing_trades += 1
                        self._gross_losses += abs(pnl)
                        self._hourly_losses.append((time.time(), abs(pnl)))
                    
                    # Remove closed positions
                    if self._positions[key].size <= 0:
                        del self._positions[key]
            
            self._total_fees += trade.fee
            self._total_trades += 1
            
            # Persist trade
            self.journal.record_trade(trade)
            
            # Check risk limits
            self._check_risk_limits()

    async def update_prices(self, price_updates: dict[str, float]) -> None:
        """Update current prices for all positions. token_id -> price."""
        async with self._lock:
            for key, pos in self._positions.items():
                token_id = key[1]
                if token_id in price_updates:
                    pos.update_price(price_updates[token_id])
            
            self._update_portfolio_value()

    # ── Risk Checks ─────────────────────────────

    def _check_risk_limits(self) -> None:
        """Check all risk limits. Activate kill switch if breached."""
        self._update_portfolio_value()
        
        # Drawdown check
        drawdown = self._peak_value - self._current_value
        drawdown_pct = drawdown / self._peak_value if self._peak_value > 0 else 0
        
        if drawdown_pct >= self.risk_config.max_drawdown_pct:
            self._activate_kill_switch(
                f"Max drawdown breached: {drawdown_pct:.2%} >= {self.risk_config.max_drawdown_pct:.2%}"
            )
            return
        
        if drawdown >= self.risk_config.max_drawdown_abs:
            self._activate_kill_switch(
                f"Max absolute drawdown: ${drawdown:.2f} >= ${self.risk_config.max_drawdown_abs:.2f}"
            )
            return
        
        # Hourly loss check
        self._clean_hourly_losses()
        hourly_loss = sum(loss for _, loss in self._hourly_losses)
        if hourly_loss >= self.risk_config.max_loss_per_hour_usdc:
            self._activate_kill_switch(
                f"Hourly loss limit: ${hourly_loss:.2f} >= ${self.risk_config.max_loss_per_hour_usdc:.2f}"
            )
            return
        
        hourly_count = len(self._hourly_losses)
        if hourly_count >= self.risk_config.max_losses_per_hour:
            self._activate_kill_switch(
                f"Hourly loss count: {hourly_count} >= {self.risk_config.max_losses_per_hour}"
            )

    def _clean_hourly_losses(self) -> None:
        """Remove losses older than 1 hour."""
        cutoff = time.time() - 3600
        while self._hourly_losses and self._hourly_losses[0][0] < cutoff:
            self._hourly_losses.popleft()

    def _update_portfolio_value(self) -> None:
        """Recalculate total portfolio value."""
        positions_value = sum(pos.notional_value for pos in self._positions.values())
        self._current_value = self._cash_balance + positions_value
        
        if self._current_value > self._peak_value:
            self._peak_value = self._current_value

    def _activate_kill_switch(self, reason: str) -> None:
        """Activate the kill switch — halt all trading."""
        if not self._kill_switch_active:
            self._kill_switch_active = True
            self._kill_switch_reason = reason
            logger.critical("KILL SWITCH ACTIVATED", reason=reason)

    def deactivate_kill_switch(self) -> None:
        """Manually deactivate kill switch."""
        self._kill_switch_active = False
        self._kill_switch_reason = ""
        logger.warning("Kill switch deactivated manually")

    # ── Pre-Trade Validation ────────────────────

    async def validate_trade(
        self,
        market_condition_id: str,
        token_id: str,
        side: OrderSide,
        size: float,
        price: float,
    ) -> tuple[bool, str]:
        """
        Validate a trade against risk limits before execution.
        Returns (allowed, reason).
        """
        if self._kill_switch_active:
            return False, f"Kill switch active: {self._kill_switch_reason}"
        
        cost = size * price
        
        # Check available capital (with reserve)
        available = self.risk_config.available_capital - self._total_exposure()
        if side == OrderSide.BUY and cost > available:
            return False, f"Insufficient capital: need ${cost:.2f}, have ${available:.2f}"
        
        # Check single trade limit
        if cost > self.risk_config.max_single_trade:
            return False, f"Trade exceeds single limit: ${cost:.2f} > ${self.risk_config.max_single_trade:.2f}"
        
        # Check per-market limit
        market_exposure = self._market_exposure(market_condition_id)
        if market_exposure + cost > self.risk_config.max_position_per_market:
            return False, f"Market limit: ${market_exposure + cost:.2f} > ${self.risk_config.max_position_per_market:.2f}"
        
        # Check total exposure
        total = self._total_exposure() + cost
        if total > self.risk_config.max_total_exposure:
            return False, f"Total exposure limit: ${total:.2f} > ${self.risk_config.max_total_exposure:.2f}"
        
        # Check position count
        if len(self._positions) >= self.risk_config.max_positions:
            key = (market_condition_id, token_id)
            if key not in self._positions:
                return False, f"Max positions: {len(self._positions)} >= {self.risk_config.max_positions}"
        
        return True, "OK"

    def _total_exposure(self) -> float:
        """Total notional exposure across all positions."""
        return sum(pos.notional_value for pos in self._positions.values())

    def _market_exposure(self, market_condition_id: str) -> float:
        """Total exposure in a specific market."""
        return sum(
            pos.notional_value
            for key, pos in self._positions.items()
            if key[0] == market_condition_id
        )

    # ── Portfolio Metrics ───────────────────────

    def _compute_sharpe(self) -> float:
        """Compute Sharpe ratio from returns history."""
        if len(self._returns_history) < 10:
            return 0.0
        
        returns = list(self._returns_history)
        mean_return = np.mean(returns)
        std_return = np.std(returns)
        
        if std_return == 0:
            return 0.0
        
        # Annualize (assuming ~1 snapshot per minute)
        annualization = math.sqrt(525600)  # Minutes in a year
        return float((mean_return / std_return) * annualization)

    def take_snapshot(self) -> PortfolioSnapshot:
        """Take a point-in-time portfolio snapshot."""
        self._update_portfolio_value()
        
        # Calculate return since last snapshot
        if self._last_snapshot_value > 0:
            ret = (self._current_value - self._last_snapshot_value) / self._last_snapshot_value
            self._returns_history.append(ret)
        self._last_snapshot_value = self._current_value
        
        drawdown = self._peak_value - self._current_value
        drawdown_pct = drawdown / self._peak_value if self._peak_value > 0 else 0
        
        positions_value = sum(pos.notional_value for pos in self._positions.values())
        unrealized = sum(pos.unrealized_pnl for pos in self._positions.values())
        max_exposure = max(
            (pos.notional_value for pos in self._positions.values()),
            default=0.0,
        )
        
        win_rate = (
            self._winning_trades / max(self._total_trades, 1)
        )
        
        profit_factor = (
            self._gross_wins / self._gross_losses
            if self._gross_losses > 0
            else float("inf") if self._gross_wins > 0 else 0.0
        )
        
        snapshot = PortfolioSnapshot(
            total_value=round(self._current_value, 4),
            cash_balance=round(self._cash_balance, 4),
            positions_value=round(positions_value, 4),
            total_realized_pnl=round(self._total_realized_pnl, 4),
            total_unrealized_pnl=round(unrealized, 4),
            total_fees=round(self._total_fees, 4),
            num_positions=len(self._positions),
            total_exposure=round(self._total_exposure(), 4),
            max_single_exposure=round(max_exposure, 4),
            peak_value=round(self._peak_value, 4),
            drawdown=round(drawdown, 4),
            drawdown_pct=round(drawdown_pct, 6),
            sharpe_ratio=round(self._compute_sharpe(), 4),
            win_rate=round(win_rate, 4),
            total_trades=self._total_trades,
            winning_trades=self._winning_trades,
            losing_trades=self._losing_trades,
            avg_win=round(self._gross_wins / max(self._winning_trades, 1), 4),
            avg_loss=round(self._gross_losses / max(self._losing_trades, 1), 4),
            profit_factor=round(profit_factor, 4) if profit_factor != float("inf") else 999.0,
        )
        
        # Persist
        self.journal.record_snapshot(snapshot)
        
        return snapshot

    # ── Event Risk ──────────────────────────────

    async def check_resolution_risk(self, markets: dict[str, Market]) -> list[str]:
        """
        Check for positions in markets nearing resolution.
        Returns list of warnings.
        """
        if not self.risk_config.reduce_near_resolution:
            return []
        
        warnings = []
        
        for key, pos in self._positions.items():
            market_id = key[0]
            market = markets.get(market_id)
            if not market or not market.end_date:
                continue
            
            # Simple time check (would need proper date parsing in production)
            # For now, flag markets that have end_date set
            if pos.size > 0:
                target_size = pos.size * self.risk_config.resolution_target_pct
                msg = (
                    f"Market {market.slug} nearing resolution. "
                    f"Position: {pos.size:.2f} shares, "
                    f"suggest reducing to {target_size:.2f}"
                )
                warnings.append(msg)
                logger.warning(msg)
        
        return warnings

    # ── Bot State Export ────────────────────────

    def export_state(self, strategy_stats: dict, ws_status: dict) -> BotState:
        """Export complete bot state for dashboard consumption."""
        snapshot = self.take_snapshot()
        
        positions_list = []
        for key, pos in self._positions.items():
            positions_list.append({
                "market": pos.market_condition_id,
                "token_id": pos.token_id,
                "outcome": pos.outcome,
                "side": pos.side.value,
                "size": pos.size,
                "avg_entry": pos.avg_entry_price,
                "current_price": pos.current_price,
                "unrealized_pnl": round(pos.unrealized_pnl, 4),
                "realized_pnl": round(pos.realized_pnl, 4),
                "total_pnl": round(pos.total_pnl, 4),
                "strategy": pos.strategy.value,
            })
        
        recent_trades = self.journal.get_recent_trades(50)
        
        arb_stats = strategy_stats.get("arbitrage", {})
        mm_stats = strategy_stats.get("market_making", {})
        
        state = BotState(
            ws_market_connected=ws_status.get("market", {}).get("connected", False),
            ws_user_connected=ws_status.get("user", {}).get("connected", False),
            uptime_seconds=ws_status.get("market", {}).get("uptime", 0),
            arb_enabled=arb_stats.get("enabled", False),
            mm_enabled=mm_stats.get("enabled", False),
            kill_switch_active=self._kill_switch_active,
            portfolio=snapshot,
            positions=positions_list,
            recent_trades=recent_trades,
            arb_opportunities_found=arb_stats.get("opportunities_found", 0),
            arb_opportunities_executed=arb_stats.get("opportunities_executed", 0),
            arb_total_profit=arb_stats.get("total_profit", 0),
            arb_avg_profit_per_trade=arb_stats.get("avg_profit", 0),
            arb_success_rate=arb_stats.get("success_rate", 0),
            mm_active_markets=mm_stats.get("active_markets", 0),
            mm_total_spread_captured=mm_stats.get("total_spread_captured", 0),
            mm_total_rebates=mm_stats.get("total_rebates", 0),
            mm_quote_refresh_count=mm_stats.get("quote_refresh_count", 0),
            mm_fill_rate=mm_stats.get("fill_rate", 0),
        )
        
        return state

    def write_state_file(self, state: BotState) -> None:
        """Write state to JSON file for dashboard to read."""
        try:
            with open(self._state_file, "w") as f:
                json.dump(state.to_dict(), f, indent=2)
        except Exception as e:
            logger.error("Failed to write state file", error=str(e))

    # ── Error Tracking ──────────────────────────

    def record_error(self) -> bool:
        """Record an error. Returns True if should pause (too many errors)."""
        self._consecutive_errors += 1
        if self._consecutive_errors >= self.risk_config.pause_on_error_count:
            self._activate_kill_switch(
                f"Too many consecutive errors: {self._consecutive_errors}"
            )
            return True
        return False

    def clear_errors(self) -> None:
        """Reset consecutive error count (called on success)."""
        self._consecutive_errors = 0

    # ── Stats ───────────────────────────────────

    def stats(self) -> dict:
        return {
            "total_value": round(self._current_value, 4),
            "cash_balance": round(self._cash_balance, 4),
            "positions_count": len(self._positions),
            "total_exposure": round(self._total_exposure(), 4),
            "realized_pnl": round(self._total_realized_pnl, 4),
            "total_fees": round(self._total_fees, 4),
            "peak_value": round(self._peak_value, 4),
            "drawdown": round(self._peak_value - self._current_value, 4),
            "kill_switch": self._kill_switch_active,
            "total_trades": self._total_trades,
            "win_rate": round(self._winning_trades / max(self._total_trades, 1), 4),
        }
