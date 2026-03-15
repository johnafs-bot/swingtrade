"""
Módulo 9 — Probabilidade
Calcula probabilidade estatística de sucesso de uma operação.
Baseado em: histórico de setups, contexto do ativo, regime, RS, vol.
NÃO usa opinião subjetiva.
"""

import logging
import numpy as np
import pandas as pd

from modules.setups import get_setup_stats
from modules.market_regime import apply_regime_to_probability
from modules.technical_analysis import get_latest_indicators
import config

logger = logging.getLogger(__name__)


def calculate_probability(
    setup: dict,
    ticker: str,
    regime: dict,
    fundamental_grade: str = "C",
) -> dict:
    """
    Calculate probability of success for a detected setup.

    Returns:
        prob_success: float 0-1
        prob_failure: float 0-1
        confidence:   float 0-1 (model confidence)
        occurrences:  int (historical similar setups)
        components:   dict of contributing factors
    """
    setup_name = setup.get("setup", "unknown")

    # 1. Base probability from setup historical stats
    stats = get_setup_stats(setup_name, regime.get("regime"))
    base_wr   = stats.get("win_rate", 0.50)
    n_trades  = stats.get("total_trades", 0)

    # 2. Regime adjustment
    regime_adj = apply_regime_to_probability(base_wr, setup_name, regime)

    # 3. Technical context adjustments
    ind = get_latest_indicators(ticker)
    tech_adj = _technical_adjustments(ind)

    # 4. Fundamental quality adjustment
    fund_adj = _fundamental_adjustment(fundamental_grade)

    # 5. Volume confirmation adjustment
    vol_adj = 0.0
    if setup.get("rvol", 1.0) >= config.BREAKOUT_VOL_MULT:
        vol_adj = 0.03

    # 6. Confidence in setup signal
    sig_confidence = float(setup.get("confidence", 0.55))

    # Compose final probability
    components = {
        "base_win_rate":      round(base_wr, 4),
        "regime_adjusted":    round(regime_adj, 4),
        "tech_adjustment":    round(tech_adj, 4),
        "fund_adjustment":    round(fund_adj, 4),
        "vol_adjustment":     round(vol_adj, 4),
        "signal_confidence":  round(sig_confidence, 4),
    }

    # Weighted average
    prob = (
        regime_adj * 0.40
        + (base_wr + tech_adj + fund_adj + vol_adj) * 0.40
        + sig_confidence * 0.20
    )
    prob = min(max(prob, 0.15), 0.88)

    # Model confidence based on historical data volume
    if n_trades >= 50:    model_conf = 0.80
    elif n_trades >= 20:  model_conf = 0.65
    elif n_trades >= 5:   model_conf = 0.50
    else:                 model_conf = 0.35  # using defaults

    return {
        "prob_success":     round(prob, 4),
        "prob_failure":     round(1 - prob, 4),
        "confidence":       round(model_conf, 4),
        "occurrences":      n_trades,
        "components":       components,
    }


def _technical_adjustments(ind: dict) -> float:
    """
    Return a small probability adjustment based on current technical context.
    Range: approx -0.08 to +0.08
    """
    adj = 0.0
    if not ind:
        return adj

    rsi   = ind.get("rsi14")
    adx   = ind.get("adx")
    trend = ind.get("trend_mid")
    mom   = ind.get("momentum")
    rs    = ind.get("rel_strength")

    # RSI in healthy range (not overbought, not oversold)
    if rsi is not None:
        if 40 <= float(rsi) <= 70:
            adj += 0.02
        elif float(rsi) > 75:
            adj -= 0.03  # overbought — higher reversal risk
        elif float(rsi) < 25:
            adj -= 0.02  # deep oversold

    # ADX — strong trend increases probability for trend-following setups
    if adx is not None and float(adx) > 25:
        adj += 0.02

    # Trend alignment
    if trend == "up":
        adj += 0.02
    elif trend == "down":
        adj -= 0.04

    # Momentum positive
    if mom is not None and float(mom) > 0.03:
        adj += 0.01

    # Relative strength vs market
    if rs is not None and float(rs) > 0.03:
        adj += 0.02
    elif rs is not None and float(rs) < -0.05:
        adj -= 0.02

    return round(adj, 4)


def _fundamental_adjustment(grade: str) -> float:
    """Probability boost/penalty based on fundamental quality."""
    mapping = {
        "A":   0.04,
        "B":   0.02,
        "C":   0.00,
        "D":  -0.03,
        "F":  -0.06,
        "N/A": 0.00,
    }
    return mapping.get(grade, 0.0)


def interpret_probability(prob: float) -> str:
    """Return human-readable interpretation of probability."""
    if prob >= 0.70:    return "Alta probabilidade"
    elif prob >= 0.60:  return "Probabilidade moderada-alta"
    elif prob >= 0.50:  return "Probabilidade moderada"
    elif prob >= 0.40:  return "Probabilidade moderada-baixa"
    else:               return "Baixa probabilidade"
