"""
polymarket_pro/dashboard.py — Streamlit Real-Time Dashboard
=============================================================
Live monitoring dashboard for the Polymarket trading bot.
Reads state from shared JSON file written by the bot.

Run: streamlit run polymarket_pro/dashboard.py
"""

from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st


# ─────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────

STATE_FILE = "data/bot_state.json"
TRADES_DB = "data/trades.db"
REFRESH_MS = 1000


# ─────────────────────────────────────────────
# Data Loading
# ─────────────────────────────────────────────

def load_bot_state() -> dict:
    """Load current bot state from JSON file."""
    try:
        path = Path(STATE_FILE)
        if path.exists():
            with open(path) as f:
                return json.load(f)
    except (json.JSONDecodeError, Exception):
        pass
    return {}


def load_trades(limit: int = 500) -> pd.DataFrame:
    """Load recent trades from SQLite."""
    try:
        if not Path(TRADES_DB).exists():
            return pd.DataFrame()
        conn = sqlite3.connect(TRADES_DB)
        df = pd.read_sql_query(
            f"SELECT * FROM trades ORDER BY timestamp DESC LIMIT {limit}",
            conn,
        )
        conn.close()
        if not df.empty and "timestamp" in df.columns:
            df["time"] = pd.to_datetime(df["timestamp"], unit="s")
        return df
    except Exception:
        return pd.DataFrame()


def load_snapshots(hours: float = 24.0) -> pd.DataFrame:
    """Load portfolio snapshots from SQLite."""
    try:
        if not Path(TRADES_DB).exists():
            return pd.DataFrame()
        cutoff = time.time() - (hours * 3600)
        conn = sqlite3.connect(TRADES_DB)
        df = pd.read_sql_query(
            f"SELECT * FROM snapshots WHERE timestamp > {cutoff} ORDER BY timestamp",
            conn,
        )
        conn.close()
        if not df.empty and "timestamp" in df.columns:
            df["time"] = pd.to_datetime(df["timestamp"], unit="s")
        return df
    except Exception:
        return pd.DataFrame()


# ─────────────────────────────────────────────
# Page Config
# ─────────────────────────────────────────────

st.set_page_config(
    page_title="Polymarket Pro Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─────────────────────────────────────────────
# Custom CSS
# ─────────────────────────────────────────────

st.markdown("""
<style>
    .stMetric {
        background-color: rgba(28, 131, 225, 0.05);
        border-radius: 8px;
        padding: 10px;
    }
    .status-green { color: #00c853; font-weight: bold; }
    .status-red { color: #ff1744; font-weight: bold; }
    .status-yellow { color: #ffab00; font-weight: bold; }
    div[data-testid="stMetricValue"] {
        font-size: 1.8rem;
    }
</style>
""", unsafe_allow_html=True)


# ─────────────────────────────────────────────
# Load Data
# ─────────────────────────────────────────────

state = load_bot_state()
portfolio = state.get("portfolio", {})


# ─────────────────────────────────────────────
# Header — Connection & Strategy Status
# ─────────────────────────────────────────────

st.title("Polymarket Pro Dashboard")

header_cols = st.columns(6)

with header_cols[0]:
    ws_market = state.get("ws_market_connected", False)
    status = "CONNECTED" if ws_market else "DISCONNECTED"
    color = "status-green" if ws_market else "status-red"
    st.markdown(f"**Market WS:** <span class='{color}'>{status}</span>", unsafe_allow_html=True)

with header_cols[1]:
    ws_user = state.get("ws_user_connected", False)
    status = "CONNECTED" if ws_user else "DISCONNECTED"
    color = "status-green" if ws_user else "status-red"
    st.markdown(f"**User WS:** <span class='{color}'>{status}</span>", unsafe_allow_html=True)

with header_cols[2]:
    arb_on = state.get("arb_enabled", False)
    color = "status-green" if arb_on else "status-red"
    st.markdown(f"**Arbitrage:** <span class='{color}'>{'ON' if arb_on else 'OFF'}</span>", unsafe_allow_html=True)

with header_cols[3]:
    mm_on = state.get("mm_enabled", False)
    color = "status-green" if mm_on else "status-red"
    st.markdown(f"**Market Making:** <span class='{color}'>{'ON' if mm_on else 'OFF'}</span>", unsafe_allow_html=True)

with header_cols[4]:
    kill = state.get("kill_switch_active", False)
    if kill:
        st.markdown("**Kill Switch:** <span class='status-red'>ACTIVE</span>", unsafe_allow_html=True)
    else:
        st.markdown("**Kill Switch:** <span class='status-green'>OFF</span>", unsafe_allow_html=True)

with header_cols[5]:
    uptime = state.get("uptime_seconds", 0)
    hours = int(uptime // 3600)
    mins = int((uptime % 3600) // 60)
    st.markdown(f"**Uptime:** {hours}h {mins}m")

st.divider()


# ─────────────────────────────────────────────
# Row 1 — Key Metrics
# ─────────────────────────────────────────────

st.subheader("Portfolio Overview")

m1, m2, m3, m4, m5, m6 = st.columns(6)

total_pnl = portfolio.get("realized_pnl", 0) + portfolio.get("unrealized_pnl", 0)
with m1:
    st.metric(
        "Total P&L",
        f"${total_pnl:,.2f}",
        delta=f"${portfolio.get('realized_pnl', 0):,.2f} realized",
    )

with m2:
    st.metric(
        "Unrealized P&L",
        f"${portfolio.get('unrealized_pnl', 0):,.2f}",
    )

with m3:
    st.metric(
        "Win Rate",
        f"{portfolio.get('win_rate', 0):.1%}",
        delta=f"{portfolio.get('total_trades', 0)} trades",
    )

with m4:
    st.metric(
        "Sharpe Ratio",
        f"{portfolio.get('sharpe_ratio', 0):.2f}",
    )

with m5:
    st.metric(
        "Active Positions",
        f"{portfolio.get('num_positions', 0)}",
        delta=f"${portfolio.get('total_exposure', 0):,.0f} deployed",
    )

with m6:
    st.metric(
        "Drawdown",
        f"{portfolio.get('drawdown_pct', 0):.2%}",
        delta=f"-${portfolio.get('drawdown', 0):,.2f}",
        delta_color="inverse",
    )

st.divider()


# ─────────────────────────────────────────────
# Row 2 — Live Positions
# ─────────────────────────────────────────────

st.subheader("Live Positions")

positions = state.get("positions", [])
if positions:
    pos_df = pd.DataFrame(positions)
    
    # Format columns
    display_cols = [
        "market", "outcome", "side", "size", "avg_entry",
        "current_price", "unrealized_pnl", "realized_pnl",
        "total_pnl", "strategy",
    ]
    available_cols = [c for c in display_cols if c in pos_df.columns]
    
    st.dataframe(
        pos_df[available_cols],
        use_container_width=True,
        height=min(400, len(positions) * 40 + 50),
        column_config={
            "unrealized_pnl": st.column_config.NumberColumn(format="$%.4f"),
            "realized_pnl": st.column_config.NumberColumn(format="$%.4f"),
            "total_pnl": st.column_config.NumberColumn(format="$%.4f"),
            "avg_entry": st.column_config.NumberColumn(format="%.4f"),
            "current_price": st.column_config.NumberColumn(format="%.4f"),
            "size": st.column_config.NumberColumn(format="%.2f"),
        },
    )
else:
    st.info("No active positions")

st.divider()


# ─────────────────────────────────────────────
# Row 3 — Strategy Performance
# ─────────────────────────────────────────────

st.subheader("Strategy Performance")

tab_arb, tab_mm = st.tabs(["Arbitrage", "Market Making"])

with tab_arb:
    a1, a2, a3, a4, a5 = st.columns(5)
    with a1:
        st.metric("Opportunities Found", state.get("arb_opportunities_found", 0))
    with a2:
        st.metric("Executed", state.get("arb_opportunities_executed", 0))
    with a3:
        st.metric("Total Profit", f"${state.get('arb_total_profit', 0):,.4f}")
    with a4:
        st.metric("Avg Profit/Trade", f"${state.get('arb_avg_profit_per_trade', 0):,.4f}")
    with a5:
        st.metric("Success Rate", f"{state.get('arb_success_rate', 0):.1%}")

with tab_mm:
    mm1, mm2, mm3, mm4, mm5 = st.columns(5)
    with mm1:
        st.metric("Active Markets", state.get("mm_active_markets", 0))
    with mm2:
        st.metric("Spread Captured", f"${state.get('mm_total_spread_captured', 0):,.4f}")
    with mm3:
        st.metric("Rebates Earned", f"${state.get('mm_total_rebates', 0):,.4f}")
    with mm4:
        st.metric("Quote Refreshes", state.get("mm_quote_refresh_count", 0))
    with mm5:
        st.metric("Fill Rate", f"{state.get('mm_fill_rate', 0):.1%}")

st.divider()


# ─────────────────────────────────────────────
# Row 4 — Charts
# ─────────────────────────────────────────────

st.subheader("Analytics")

chart_col1, chart_col2 = st.columns(2)

# Cumulative P&L Chart
with chart_col1:
    st.markdown("**Cumulative P&L**")
    snapshots_df = load_snapshots(24.0)
    
    if not snapshots_df.empty and "realized_pnl" in snapshots_df.columns:
        fig_pnl = go.Figure()
        
        fig_pnl.add_trace(go.Scatter(
            x=snapshots_df["time"],
            y=snapshots_df["realized_pnl"] + snapshots_df.get("unrealized_pnl", 0),
            mode="lines",
            name="Total P&L",
            line=dict(color="#00c853", width=2),
            fill="tozeroy",
            fillcolor="rgba(0, 200, 83, 0.1)",
        ))
        
        fig_pnl.add_trace(go.Scatter(
            x=snapshots_df["time"],
            y=snapshots_df["realized_pnl"],
            mode="lines",
            name="Realized P&L",
            line=dict(color="#2196f3", width=1.5, dash="dash"),
        ))
        
        fig_pnl.update_layout(
            height=350,
            margin=dict(l=0, r=0, t=10, b=0),
            legend=dict(orientation="h", yanchor="bottom", y=1.02),
            xaxis_title="",
            yaxis_title="USDC",
            template="plotly_dark",
        )
        st.plotly_chart(fig_pnl, use_container_width=True)
    else:
        st.info("No P&L data yet — waiting for snapshots")

# Drawdown Chart
with chart_col2:
    st.markdown("**Drawdown**")
    
    if not snapshots_df.empty and "drawdown_pct" in snapshots_df.columns:
        fig_dd = go.Figure()
        
        fig_dd.add_trace(go.Scatter(
            x=snapshots_df["time"],
            y=snapshots_df["drawdown_pct"] * -100,
            mode="lines",
            name="Drawdown %",
            line=dict(color="#ff1744", width=2),
            fill="tozeroy",
            fillcolor="rgba(255, 23, 68, 0.1)",
        ))
        
        fig_dd.update_layout(
            height=350,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="",
            yaxis_title="Drawdown %",
            template="plotly_dark",
        )
        st.plotly_chart(fig_dd, use_container_width=True)
    else:
        st.info("No drawdown data yet")

# Second row of charts
chart_col3, chart_col4 = st.columns(2)

# Position Exposure
with chart_col3:
    st.markdown("**Position Exposure by Market**")
    if positions:
        pos_df_chart = pd.DataFrame(positions)
        if "market" in pos_df_chart.columns and "size" in pos_df_chart.columns:
            # Truncate market names for readability
            pos_df_chart["market_short"] = pos_df_chart["market"].str[:16] + "..."
            
            fig_exp = px.bar(
                pos_df_chart,
                x="market_short",
                y="size",
                color="strategy",
                title="",
                template="plotly_dark",
                color_discrete_map={
                    "arbitrage": "#00c853",
                    "market_making": "#2196f3",
                    "manual": "#ff9800",
                },
            )
            fig_exp.update_layout(
                height=300,
                margin=dict(l=0, r=0, t=10, b=0),
                xaxis_title="",
                yaxis_title="Size (USDC)",
            )
            st.plotly_chart(fig_exp, use_container_width=True)
        else:
            st.info("No position data to display")
    else:
        st.info("No positions")

# Trade P&L Distribution
with chart_col4:
    st.markdown("**Trade P&L Distribution**")
    trades_df = load_trades(200)
    
    if not trades_df.empty and "price" in trades_df.columns and "size" in trades_df.columns:
        trades_df["notional"] = trades_df["price"] * trades_df["size"]
        
        fig_dist = px.histogram(
            trades_df,
            x="notional",
            nbins=30,
            title="",
            template="plotly_dark",
            color_discrete_sequence=["#2196f3"],
        )
        fig_dist.update_layout(
            height=300,
            margin=dict(l=0, r=0, t=10, b=0),
            xaxis_title="Trade Size (USDC)",
            yaxis_title="Count",
        )
        st.plotly_chart(fig_dist, use_container_width=True)
    else:
        st.info("No trade data yet")

st.divider()


# ─────────────────────────────────────────────
# Row 5 — Trade History
# ─────────────────────────────────────────────

st.subheader("Recent Trades")

trades_display = state.get("recent_trades", [])
if trades_display:
    trades_tbl = pd.DataFrame(trades_display)
    
    # Filter controls
    filter_cols = st.columns(3)
    with filter_cols[0]:
        strat_filter = st.selectbox(
            "Strategy",
            ["All"] + list(trades_tbl["strategy"].unique()) if "strategy" in trades_tbl.columns else ["All"],
            key="strat_filter",
        )
    with filter_cols[1]:
        side_filter = st.selectbox(
            "Side",
            ["All"] + list(trades_tbl["side"].unique()) if "side" in trades_tbl.columns else ["All"],
            key="side_filter",
        )
    with filter_cols[2]:
        limit_filter = st.slider("Show", 10, 50, 50, key="limit_filter")
    
    # Apply filters
    filtered = trades_tbl.copy()
    if strat_filter != "All" and "strategy" in filtered.columns:
        filtered = filtered[filtered["strategy"] == strat_filter]
    if side_filter != "All" and "side" in filtered.columns:
        filtered = filtered[filtered["side"] == side_filter]
    
    filtered = filtered.head(limit_filter)
    
    st.dataframe(
        filtered,
        use_container_width=True,
        height=min(400, len(filtered) * 40 + 50),
        column_config={
            "price": st.column_config.NumberColumn(format="%.4f"),
            "size": st.column_config.NumberColumn(format="%.2f"),
            "fee": st.column_config.NumberColumn(format="$%.4f"),
        },
    )
else:
    st.info("No trades recorded yet")

st.divider()


# ─────────────────────────────────────────────
# Row 6 — Order Book Viewer
# ─────────────────────────────────────────────

st.subheader("Order Book Viewer")

# Note: In production, this would read from the WebSocket cache
# For now, show a placeholder with instructions
st.markdown("""
*Order book data is streamed via WebSocket and cached by the bot.*
*Connect the bot to see live order book data here.*
""")

ob_col1, ob_col2 = st.columns(2)

with ob_col1:
    st.markdown("**Bids (Buy Orders)**")
    # Placeholder table
    sample_bids = pd.DataFrame({
        "Price": [0.55, 0.54, 0.53, 0.52, 0.51],
        "Size": [100, 250, 180, 320, 150],
        "Total": [100, 350, 530, 850, 1000],
    })
    st.dataframe(sample_bids, use_container_width=True, hide_index=True)

with ob_col2:
    st.markdown("**Asks (Sell Orders)**")
    sample_asks = pd.DataFrame({
        "Price": [0.56, 0.57, 0.58, 0.59, 0.60],
        "Size": [120, 200, 160, 280, 190],
        "Total": [120, 320, 480, 760, 950],
    })
    st.dataframe(sample_asks, use_container_width=True, hide_index=True)


# ─────────────────────────────────────────────
# Sidebar — Controls
# ─────────────────────────────────────────────

with st.sidebar:
    st.header("Controls")
    
    st.subheader("Strategy Toggle")
    st.toggle("Arbitrage Enabled", value=state.get("arb_enabled", False), disabled=True, key="arb_toggle")
    st.toggle("Market Making Enabled", value=state.get("mm_enabled", False), disabled=True, key="mm_toggle")
    
    st.divider()
    
    st.subheader("Risk Parameters")
    st.markdown(f"""
    | Parameter | Value |
    |-----------|-------|
    | Total Capital | ${portfolio.get('total_value', 0):,.2f} |
    | Cash Balance | ${portfolio.get('cash_balance', 0):,.2f} |
    | Max Drawdown | {portfolio.get('drawdown_pct', 0):.2%} |
    | Positions | {portfolio.get('num_positions', 0)} |
    | Exposure | ${portfolio.get('total_exposure', 0):,.2f} |
    """)
    
    st.divider()
    
    st.subheader("Errors")
    errors = state.get("recent_errors", [])
    error_count = state.get("error_count", 0)
    st.markdown(f"**Total Errors:** {error_count}")
    if errors:
        for err in errors[-5:]:
            st.error(err)
    else:
        st.success("No recent errors")
    
    st.divider()
    
    st.subheader("Last Updated")
    last_updated = state.get("last_updated", 0)
    if last_updated:
        dt = datetime.fromtimestamp(last_updated)
        st.markdown(f"{dt.strftime('%Y-%m-%d %H:%M:%S')}")
        age = time.time() - last_updated
        if age > 10:
            st.warning(f"Data is {age:.0f}s old")
    else:
        st.warning("No data received yet")

    st.divider()
    st.caption("Polymarket Pro v2.0")


# ─────────────────────────────────────────────
# Auto-refresh
# ─────────────────────────────────────────────

st.markdown(
    f"""
    <script>
        setTimeout(function() {{
            window.location.reload();
        }}, {REFRESH_MS});
    </script>
    """,
    unsafe_allow_html=True,
)
