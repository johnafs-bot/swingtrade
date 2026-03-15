"""
Módulo 13 — Carteira
Gerencia posições abertas, exposição e risco consolidado.
"""

import logging
from datetime import datetime
import pandas as pd
import yfinance as yf

from database.connection import get_connection
import config

logger = logging.getLogger(__name__)


def get_portfolio() -> list:
    """Return all open positions."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM portfolio WHERE status='open' ORDER BY date_opened DESC"
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_user_profile() -> dict:
    """Return user profile (first row)."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM user_profile ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if row:
            return dict(row)
        # Default profile
        return {
            "total_capital":        100000,
            "free_capital":         100000,
            "horizon_days":         config.MAX_HORIZON_DAYS,
            "risk_per_trade_pct":   config.RISK_PER_TRADE_PCT,
            "max_loss_pct":         10.0,
            "max_positions":        config.MAX_POSITIONS,
            "forbidden_sectors":    "",
            "volatility_tolerance": "medium",
            "strategy":             config.DEFAULT_STRATEGY,
        }
    finally:
        conn.close()


def save_user_profile(profile: dict) -> bool:
    """Save or update user profile."""
    conn = get_connection()
    try:
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        conn.execute(
            """INSERT OR REPLACE INTO user_profile
               (id, total_capital, free_capital, horizon_days, risk_per_trade_pct,
                max_loss_pct, max_positions, forbidden_sectors, volatility_tolerance,
                preferred_setups, strategy, updated_at)
               VALUES (1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                profile.get("total_capital", 100000),
                profile.get("free_capital", 100000),
                profile.get("horizon_days", 90),
                profile.get("risk_per_trade_pct", 1.0),
                profile.get("max_loss_pct", 10.0),
                profile.get("max_positions", 10),
                profile.get("forbidden_sectors", ""),
                profile.get("volatility_tolerance", "medium"),
                profile.get("preferred_setups", "all"),
                profile.get("strategy", "mixed"),
                now,
            )
        )
        conn.commit()
        return True
    except Exception as e:
        logger.error(f"Save profile error: {e}")
        return False
    finally:
        conn.close()


def add_position(ticker: str, quantity: float, avg_price: float,
                  stop_price: float = None, target_price: float = None,
                  setup_name: str = None, notes: str = None) -> int:
    """Add new open position. Returns new position id."""
    conn = get_connection()
    try:
        today = datetime.today().strftime("%Y-%m-%d")
        cur = conn.execute(
            """INSERT INTO portfolio
               (ticker, quantity, avg_price, current_price, date_opened,
                stop_price, target_price, setup_name, status, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'open', ?)""",
            (ticker, quantity, avg_price, avg_price, today,
             stop_price, target_price, setup_name, notes)
        )
        conn.commit()
        # Update free capital
        _deduct_capital(avg_price * quantity, conn)
        conn.commit()
        return cur.lastrowid
    finally:
        conn.close()


def close_position(position_id: int, close_price: float) -> dict:
    """Close a position and record PnL."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM portfolio WHERE id=?", (position_id,)
        ).fetchone()
        if not row:
            return {"error": "Position not found"}

        pos     = dict(row)
        qty     = pos["quantity"]
        avg     = pos["avg_price"]
        pnl     = (close_price - avg) * qty
        pnl_pct = (close_price - avg) / avg * 100

        conn.execute(
            """UPDATE portfolio SET status='closed', closed_at=?, current_price=?,
               pnl=?, pnl_pct=? WHERE id=?""",
            (datetime.today().strftime("%Y-%m-%d"), close_price,
             round(pnl, 2), round(pnl_pct, 2), position_id)
        )
        conn.commit()

        # Restore capital
        _restore_capital(close_price * qty, conn)
        conn.commit()

        return {"status": "closed", "pnl": round(pnl, 2), "pnl_pct": round(pnl_pct, 2)}
    finally:
        conn.close()


def update_current_prices():
    """Fetch and update current prices for all open positions."""
    positions = get_portfolio()
    if not positions:
        return

    conn = get_connection()
    try:
        for pos in positions:
            ticker = pos["ticker"]
            try:
                df = yf.download(
                    ticker + ".SA", period="2d",
                    auto_adjust=True, progress=False
                )
                if not df.empty:
                    price = float(df["Close"].iloc[-1])
                    conn.execute(
                        "UPDATE portfolio SET current_price=? WHERE id=?",
                        (round(price, 2), pos["id"])
                    )
            except Exception as e:
                logger.debug(f"Price update error {ticker}: {e}")
        conn.commit()
    finally:
        conn.close()


def get_portfolio_summary() -> dict:
    """Return comprehensive portfolio summary."""
    positions = get_portfolio()
    profile   = get_user_profile()

    if not positions:
        return {
            "positions":      [],
            "n_positions":    0,
            "total_invested": 0,
            "total_value":    0,
            "total_pnl":      0,
            "total_pnl_pct":  0,
            "free_capital":   profile.get("free_capital", 0),
            "total_capital":  profile.get("total_capital", 0),
        }

    total_cost  = sum(p["avg_price"] * p["quantity"] for p in positions)
    total_value = sum((p.get("current_price") or p["avg_price"]) * p["quantity"]
                      for p in positions)
    total_pnl   = total_value - total_cost
    total_pnl_pct = (total_pnl / total_cost * 100) if total_cost > 0 else 0

    # Sector breakdown
    conn = get_connection()
    sector_map = {}
    for p in positions:
        row = conn.execute("SELECT sector FROM assets WHERE ticker=?", (p["ticker"],)).fetchone()
        sector_map[p["ticker"]] = row["sector"] if row else "Outros"
    conn.close()

    sector_exposure = {}
    for p in positions:
        s = sector_map.get(p["ticker"], "Outros")
        val = (p.get("current_price") or p["avg_price"]) * p["quantity"]
        sector_exposure[s] = sector_exposure.get(s, 0) + val

    return {
        "positions":         positions,
        "n_positions":       len(positions),
        "total_invested":    round(total_cost, 2),
        "total_value":       round(total_value, 2),
        "total_pnl":         round(total_pnl, 2),
        "total_pnl_pct":     round(total_pnl_pct, 2),
        "free_capital":      profile.get("free_capital", 0),
        "total_capital":     profile.get("total_capital", 0),
        "sector_exposure":   {k: round(v, 2) for k, v in sector_exposure.items()},
        "sector_map":        sector_map,
    }


def _deduct_capital(amount: float, conn=None):
    """Reduce free_capital after buying."""
    close_conn = conn is None
    if close_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE user_profile SET free_capital = MAX(0, free_capital - ?) WHERE id=1",
            (amount,)
        )
    finally:
        if close_conn:
            conn.commit()
            conn.close()


def _restore_capital(amount: float, conn=None):
    """Restore free_capital after selling."""
    close_conn = conn is None
    if close_conn:
        conn = get_connection()
    try:
        conn.execute(
            "UPDATE user_profile SET free_capital = free_capital + ? WHERE id=1",
            (amount,)
        )
    finally:
        if close_conn:
            conn.commit()
            conn.close()


def get_open_tickers() -> list:
    """Return list of tickers with open positions."""
    return [p["ticker"] for p in get_portfolio()]
