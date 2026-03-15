"""
Módulo 14 — Position Sizing
Dimensiona posições com base em risco, não em capital disponível.
"""

import logging
import math

from modules.portfolio import get_user_profile
from modules.technical_analysis import get_latest_indicators
from modules.math_expectation import calculate_kelly_fraction
import config

logger = logging.getLogger(__name__)


def calculate_position_size(
    ticker: str,
    entry_price: float,
    stop_price: float,
    portfolio_value: float = None,
    free_capital: float = None,
    risk_pct: float = None,
    regime_aggression: float = 1.0,
    correlation: float = 0.0,
    prob_success: float = 0.55,
    payoff: float = 2.0,
) -> dict:
    """
    Calculate ideal position size using risk-based method.

    Risk-based sizing: Quantity = (Portfolio * Risk%) / (Entry - Stop)
    Adjustments: volatility, regime, correlation, Kelly.
    """
    profile = get_user_profile()

    port_val   = portfolio_value or profile.get("total_capital", 100000)
    free_cap   = free_capital   or profile.get("free_capital", 100000)
    risk_pct_s = risk_pct       or profile.get("risk_per_trade_pct", config.RISK_PER_TRADE_PCT)

    risk_monetary = port_val * (risk_pct_s / 100)
    risk_per_share = entry_price - stop_price

    if risk_per_share <= 0:
        return {"error": "Stop must be below entry", "quantity": 0}

    # Base quantity from risk
    base_qty = risk_monetary / risk_per_share

    # ── Adjustments ──────────────────────────────────────────────────────────

    # 1. Volatility adjustment (higher vol = smaller position)
    ind = get_latest_indicators(ticker)
    vol = ind.get("vol_hist")
    if vol:
        vol_f = float(vol)
        if vol_f > 0.50:        vol_mult = 0.60
        elif vol_f > 0.35:      vol_mult = 0.75
        elif vol_f > 0.25:      vol_mult = 0.90
        elif vol_f > 0.15:      vol_mult = 1.00
        else:                   vol_mult = 1.10  # low vol = slightly bigger
    else:
        vol_mult = 1.0

    # 2. Regime aggression adjustment
    reg_mult = float(regime_aggression) if regime_aggression else 1.0
    reg_mult = max(0.3, min(reg_mult, 1.2))

    # 3. Correlation penalty (high correlation = reduce)
    corr_mult = 1.0 - (correlation * 0.3)  # up to 30% reduction at corr=1
    corr_mult = max(0.5, corr_mult)

    # 4. Kelly constraint
    kelly_f   = calculate_kelly_fraction(prob_success, payoff)
    kelly_cap = kelly_f * port_val / entry_price  # max qty from Kelly
    kelly_mult = 1.0  # Kelly is a cap, not a direct multiplier here

    # Apply all multipliers
    adjusted_qty = base_qty * vol_mult * reg_mult * corr_mult

    # Cap by Kelly
    if kelly_cap > 0:
        adjusted_qty = min(adjusted_qty, kelly_cap)

    # Round down to whole shares
    quantity     = max(1, math.floor(adjusted_qty))
    capital_req  = quantity * entry_price

    # Cap by free capital
    if capital_req > free_cap:
        quantity    = max(1, math.floor(free_cap / entry_price))
        capital_req = quantity * entry_price

    # Cap by max single asset exposure
    max_single   = port_val * (config.MAX_SINGLE_ASSET_PCT / 100)
    if capital_req > max_single:
        quantity    = max(1, math.floor(max_single / entry_price))
        capital_req = quantity * entry_price

    actual_risk_mon = risk_per_share * quantity
    actual_risk_pct = (actual_risk_mon / port_val) * 100
    position_pct    = (capital_req / port_val) * 100

    return {
        "ticker":            ticker,
        "entry_price":       round(entry_price, 2),
        "stop_price":        round(stop_price, 2),
        "quantity":          quantity,
        "capital_required":  round(capital_req, 2),
        "position_pct":      round(position_pct, 2),
        "risk_monetary":     round(actual_risk_mon, 2),
        "risk_pct":          round(actual_risk_pct, 3),
        "vol_mult":          round(vol_mult, 3),
        "regime_mult":       round(reg_mult, 3),
        "corr_mult":         round(corr_mult, 3),
        "kelly_fraction":    round(kelly_f, 4),
        "free_capital":      round(free_cap, 2),
        "feasible":          quantity > 0 and capital_req <= free_cap,
    }


def suggest_size_label(position_pct: float) -> str:
    """Translate position % into a human-readable size label."""
    if position_pct <= 3:     return "comprar_pequeno"
    elif position_pct <= 8:   return "comprar_normal"
    elif position_pct <= 12:  return "comprar_grande"
    else:                     return "comprar_normal"  # never auto-suggest grande
