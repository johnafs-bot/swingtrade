"""
Módulo 12 — Expectativa Matemática
Calcula expectativa matemática de cada operação e classifica o trade.
EM = P(win) * Ganho_médio - P(loss) * Perda_média
"""

import logging
import config

logger = logging.getLogger(__name__)


def calculate_expected_value(
    prob_success: float,
    avg_gain_pct: float,
    avg_loss_pct: float,
) -> dict:
    """
    Calculate mathematical expectation in R (multiples of risk).

    Args:
        prob_success:  probability of winning (0-1)
        avg_gain_pct:  average gain percentage (positive)
        avg_loss_pct:  average loss percentage (negative)

    Returns:
        dict with expected_value, payoff, classification
    """
    prob_failure = 1.0 - prob_success

    # Convert to R multiples (if avg_loss = 3%, gains in those units)
    loss_abs = abs(avg_loss_pct)
    if loss_abs == 0:
        loss_abs = 1.0  # avoid division by zero

    payoff = avg_gain_pct / loss_abs  # R:R ratio

    # Expected value in % terms
    ev_pct = prob_success * avg_gain_pct + prob_failure * avg_loss_pct

    # Expected value in R (multiples of risk)
    ev_r = prob_success * payoff - prob_failure * 1.0

    # Classification
    if ev_r >= 0.5:
        classification = "expectativa_positiva_forte"
        label          = "Expectativa Fortemente Positiva"
    elif ev_r >= config.MIN_EXP_VALUE:
        classification = "expectativa_positiva"
        label          = "Expectativa Positiva"
    elif ev_r >= -0.1:
        classification = "expectativa_neutra"
        label          = "Expectativa Neutra"
    else:
        classification = "expectativa_negativa"
        label          = "Expectativa Negativa"

    return {
        "prob_success":     round(prob_success, 4),
        "prob_failure":     round(prob_failure, 4),
        "avg_gain_pct":     round(avg_gain_pct, 2),
        "avg_loss_pct":     round(avg_loss_pct, 2),
        "payoff":           round(payoff, 2),
        "expected_value_pct": round(ev_pct, 3),
        "expected_value_r": round(ev_r, 3),
        "classification":   classification,
        "label":            label,
        "is_positive":      ev_r >= config.MIN_EXP_VALUE,
        "is_tradeable":     (ev_r >= config.MIN_EXP_VALUE and
                             prob_success >= config.MIN_WIN_RATE and
                             payoff >= config.MIN_RISK_REWARD),
    }


def calculate_kelly_fraction(prob_success: float, payoff: float) -> float:
    """
    Kelly Criterion fraction for position sizing.
    f = (p * b - q) / b  where b = payoff, p = win rate, q = 1-p
    Returns conservative half-Kelly.
    """
    q = 1.0 - prob_success
    if payoff <= 0:
        return 0.01

    kelly = (prob_success * payoff - q) / payoff
    kelly = max(0, kelly)

    # Half-Kelly for safety
    half_kelly = kelly / 2.0
    return round(min(half_kelly, 0.25), 4)  # cap at 25%


def minimum_win_rate_for_profitability(payoff: float) -> float:
    """
    Calculate minimum win rate needed for a trade to be profitable
    given its payoff ratio.
    breakeven: p * payoff = (1-p) * 1  →  p = 1 / (1 + payoff)
    """
    if payoff <= 0:
        return 1.0
    return round(1.0 / (1.0 + payoff), 4)


def score_opportunity(ev_result: dict, setup: dict, regime: dict) -> float:
    """
    Composite opportunity score (0-100) for ranking.
    """
    if not ev_result.get("is_tradeable"):
        base = 0
    else:
        ev_r  = ev_result.get("expected_value_r", 0)
        prob  = ev_result.get("prob_success", 0)
        rr    = ev_result.get("payoff", 1)
        agg   = regime.get("aggression", 0.7)

        # Components (each 0-25 points)
        ev_score   = min(ev_r * 20, 25)          # max at EV=1.25R
        prob_score = (prob - 0.45) / 0.40 * 25   # 45% → 0, 85% → 25
        rr_score   = min((rr - 1.5) / 2.0 * 25, 25)  # 1.5:1 → 0, 3.5:1 → 25
        reg_score  = agg * 25                     # 0.3 → 7.5, 1.1 → 27.5 (capped)

        prob_score = max(0, min(prob_score, 25))
        rr_score   = max(0, min(rr_score, 25))
        reg_score  = max(0, min(reg_score, 25))

        base = ev_score + prob_score + rr_score + reg_score

    return round(max(0, min(base, 100)), 1)
