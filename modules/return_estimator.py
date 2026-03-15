"""
Módulo 11 — Estimativa de Retorno
Calcula alvo técnico, retorno esperado, payoff e tempo médio da operação.
"""

import logging
import numpy as np
import pandas as pd

from modules.data_collector import get_ohlcv_df
from modules.technical_analysis import get_latest_indicators
from modules.setups import get_setup_stats
import config

logger = logging.getLogger(__name__)


def estimate_return(
    ticker: str,
    entry_price: float,
    stop_price: float,
    target_price: float,
    quantity: int,
    setup_name: str,
    probability: float,
) -> dict:
    """
    Full return estimation for an operation.
    """
    risk_per_share   = entry_price - stop_price
    gain_per_share   = target_price - entry_price

    return_pct       = gain_per_share / entry_price * 100
    risk_pct_share   = risk_per_share / entry_price * 100

    rr_ratio         = gain_per_share / risk_per_share if risk_per_share > 0 else 0

    monetary_gain    = gain_per_share * quantity
    monetary_risk    = risk_per_share * quantity

    # Expected return based on stats
    stats = get_setup_stats(setup_name)
    avg_gain_pct = stats.get("avg_gain_pct", return_pct)
    avg_loss_pct = stats.get("avg_loss_pct", -risk_pct_share)
    avg_duration = stats.get("avg_duration", 15)

    # Expected monetary gain/loss (using stats if available, else calculated)
    exp_return_pct = (
        probability * avg_gain_pct +
        (1 - probability) * avg_loss_pct
    )

    # Annualized return estimate
    if avg_duration > 0:
        annualized = ((1 + exp_return_pct / 100) ** (252 / avg_duration) - 1) * 100
    else:
        annualized = 0.0

    return {
        "ticker":            ticker,
        "entry_price":       round(entry_price, 2),
        "stop_price":        round(stop_price, 2),
        "target_price":      round(target_price, 2),
        "quantity":          quantity,
        "return_pct":        round(return_pct, 2),
        "risk_pct":          round(risk_pct_share, 2),
        "rr_ratio":          round(rr_ratio, 2),
        "monetary_gain":     round(monetary_gain, 2),
        "monetary_risk":     round(monetary_risk, 2),
        "avg_gain_pct":      round(avg_gain_pct, 2),
        "avg_loss_pct":      round(avg_loss_pct, 2),
        "expected_return_pct":round(exp_return_pct, 2),
        "avg_duration_days": avg_duration,
        "annualized_return": round(annualized, 2),
        "rr_ok":             rr_ratio >= config.MIN_RISK_REWARD,
    }


def calculate_technical_target(ticker: str, entry_price: float,
                                 setup_name: str, stop_price: float = None) -> float:
    """
    Estimate technical price target using:
    1. Recent resistance levels
    2. ATR multiple projection
    3. Fibonacci extension
    """
    df = get_ohlcv_df(ticker, days=120)
    ind = get_latest_indicators(ticker)

    if df.empty:
        return entry_price * 1.06  # fallback: 6%

    # Method 1: resistance from recent highs
    recent_high = float(df["high"].tail(60).max())
    if recent_high > entry_price * 1.015:
        resistance_target = recent_high
    else:
        resistance_target = None

    # Method 2: ATR projection
    atr = ind.get("atr14")
    if atr:
        atr_target = entry_price + float(atr) * 3.0
    else:
        atr_target = entry_price * 1.06

    # Method 3: R:R based (using stop)
    if stop_price:
        rr_target = entry_price + (entry_price - stop_price) * config.MIN_RISK_REWARD
    else:
        rr_target = entry_price * 1.05

    # Pick most conservative non-trivial target
    candidates = [t for t in [resistance_target, atr_target, rr_target]
                  if t is not None and t > entry_price * 1.01]

    if not candidates:
        return round(entry_price * 1.05, 2)

    return round(min(candidates), 2)
