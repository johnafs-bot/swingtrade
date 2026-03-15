"""
Módulo 8 — Regime de Mercado
Classifica o contexto geral do mercado B3.
Influencia pesos dos setups e agressividade de alocação.
"""

import logging
from datetime import datetime
import numpy as np
import pandas as pd

from database.connection import get_connection
from modules.data_collector import get_ohlcv_df
import config

logger = logging.getLogger(__name__)


REGIME_LABELS = {
    "bull_trend":     "Tendência de Alta",
    "bear_trend":     "Tendência de Baixa",
    "lateral":        "Lateralização",
    "high_vol":       "Alta Volatilidade",
    "low_vol":        "Baixa Volatilidade",
    "breakout_env":   "Ambiente de Rompimento",
    "reversal_env":   "Ambiente de Reversão",
    "defensive":      "Modo Defesa",
}

# Regime modifiers for setup weights and allocation aggression
REGIME_MODIFIERS = {
    "bull_trend":   {"aggression": 1.0,  "breakout_mult": 1.2, "reversal_mult": 0.8},
    "bear_trend":   {"aggression": 0.4,  "breakout_mult": 0.5, "reversal_mult": 0.7},
    "lateral":      {"aggression": 0.7,  "breakout_mult": 1.1, "reversal_mult": 1.0},
    "high_vol":     {"aggression": 0.6,  "breakout_mult": 0.8, "reversal_mult": 0.9},
    "low_vol":      {"aggression": 0.9,  "breakout_mult": 1.1, "reversal_mult": 0.9},
    "breakout_env": {"aggression": 1.1,  "breakout_mult": 1.3, "reversal_mult": 0.7},
    "reversal_env": {"aggression": 0.8,  "breakout_mult": 0.7, "reversal_mult": 1.3},
    "defensive":    {"aggression": 0.3,  "breakout_mult": 0.4, "reversal_mult": 0.6},
}


def classify_regime() -> dict:
    """
    Classify current market regime based on Ibovespa (BOVA11) data.
    Returns dict with regime, description, and modifiers.
    """
    bench_df = get_ohlcv_df("BOVA11", days=300)

    if bench_df.empty or len(bench_df) < 60:
        # Fallback: return neutral
        return _build_regime("lateral", bench_df)

    c = bench_df["close"]
    h = bench_df["high"]
    l = bench_df["low"]

    # Moving averages
    sma20 = c.rolling(20).mean()
    sma50 = c.rolling(50).mean()

    last_close = float(c.iloc[-1])
    last_sma20 = float(sma20.iloc[-1])
    last_sma50 = float(sma50.iloc[-1]) if len(c) >= 50 else last_sma20

    # Volatility (20d annualized)
    log_ret  = np.log(c / c.shift(1)).dropna()
    vol_20d  = float(log_ret.tail(20).std() * np.sqrt(252)) if len(log_ret) >= 20 else 0.20

    # Trend classification
    above_sma20 = last_close > last_sma20
    above_sma50 = last_close > last_sma50
    sma20_up    = float(sma20.iloc[-1]) > float(sma20.iloc[-5]) if len(sma20) >= 5 else True

    # ATR for vol context
    atr = float(
        pd.Series(
            [max(h.iloc[i] - l.iloc[i],
                 abs(h.iloc[i] - c.iloc[i-1]),
                 abs(l.iloc[i] - c.iloc[i-1]))
             for i in range(1, len(c))]
        ).tail(14).mean()
    ) if len(c) > 14 else 0

    # Advance/decline proxy: % of last 20 closes up vs down
    daily_ret    = c.pct_change().tail(20).dropna()
    advance_rate = float((daily_ret > 0).sum() / len(daily_ret)) if len(daily_ret) > 0 else 0.5

    # Recent momentum (20d)
    mom_20d = float((c.iloc[-1] - c.iloc[-21]) / c.iloc[-21]) if len(c) >= 21 else 0

    # ── Regime Logic ──────────────────────────────────────────────────────────
    # 1. Defensive — extreme bear with high vol
    if not above_sma50 and vol_20d > 0.35 and mom_20d < -0.08:
        regime = "defensive"

    # 2. Bear trend
    elif not above_sma20 and not above_sma50 and not sma20_up:
        regime = "bear_trend"

    # 3. Bull trend
    elif above_sma20 and above_sma50 and sma20_up and advance_rate > 0.55:
        if vol_20d < 0.18 and mom_20d > 0.03:
            regime = "breakout_env"
        else:
            regime = "bull_trend"

    # 4. Low volatility (potential breakout)
    elif vol_20d < 0.15:
        regime = "low_vol"

    # 5. High volatility
    elif vol_20d > 0.30:
        regime = "high_vol"

    # 6. Reversal environment
    elif not above_sma20 and advance_rate > 0.50:
        regime = "reversal_env"

    # 7. Default: lateral
    else:
        regime = "lateral"

    result = _build_regime(regime, bench_df,
                            ibov_sma20=last_sma20, ibov_sma50=last_sma50,
                            ibov_close=last_close, avg_vol=vol_20d,
                            advance_decline=advance_rate)
    _store_regime(result)
    return result


def _build_regime(regime: str, bench_df: pd.DataFrame,
                   ibov_sma20=None, ibov_sma50=None, ibov_close=None,
                   avg_vol=None, advance_decline=None) -> dict:
    mods = REGIME_MODIFIERS.get(regime, REGIME_MODIFIERS["lateral"])
    return {
        "regime":          regime,
        "label":           REGIME_LABELS.get(regime, regime),
        "aggression":      mods["aggression"],
        "breakout_mult":   mods["breakout_mult"],
        "reversal_mult":   mods["reversal_mult"],
        "ibov_sma20":      ibov_sma20,
        "ibov_sma50":      ibov_sma50,
        "ibov_close":      ibov_close,
        "avg_volatility":  avg_vol,
        "advance_decline": advance_decline,
        "date":            datetime.today().strftime("%Y-%m-%d"),
    }


def _store_regime(result: dict):
    """Store regime in DB."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO market_regime
               (date, regime, ibov_trend, ibov_sma20, ibov_sma50, ibov_close,
                advance_decline, avg_volatility, description)
               VALUES (?,?,?,?,?,?,?,?,?)""",
            (
                result["date"],
                result["regime"],
                result["label"],
                result.get("ibov_sma20"),
                result.get("ibov_sma50"),
                result.get("ibov_close"),
                result.get("advance_decline"),
                result.get("avg_volatility"),
                result["label"],
            )
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Store regime error: {e}")
    finally:
        conn.close()


def get_current_regime() -> dict:
    """
    Return today's regime from DB, or classify fresh if not available.
    """
    today = datetime.today().strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM market_regime WHERE date=? ORDER BY id DESC LIMIT 1",
            (today,)
        ).fetchone()
        if row:
            r = dict(row)
            mods = REGIME_MODIFIERS.get(r["regime"], REGIME_MODIFIERS["lateral"])
            r.update(mods)
            r["label"] = REGIME_LABELS.get(r["regime"], r["regime"])
            return r
    finally:
        conn.close()

    # Not in DB — compute now
    return classify_regime()


def get_regime_history(days: int = 30) -> list:
    """Return last N days of regime history."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """SELECT * FROM market_regime
               ORDER BY date DESC LIMIT ?""",
            (days,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def apply_regime_to_probability(base_prob: float, setup_type: str, regime: dict) -> float:
    """
    Adjust probability of success based on regime.
    Breakout setups get boosted in bull/breakout, penalized in bear.
    """
    mod = 1.0
    regime_name = regime.get("regime", "lateral")

    if "breakout" in setup_type or "momentum" in setup_type:
        mod = regime.get("breakout_mult", 1.0)
    elif "reversao" in setup_type or "pullback" in setup_type:
        mod = regime.get("reversal_mult", 1.0)

    adjusted = base_prob * mod
    return min(max(adjusted, 0.10), 0.95)
