"""
Módulo 16 — Ranking Diário
Ordena ativos por qualidade do sinal, expectativa e encaixe na carteira.
"""

import logging
from datetime import datetime
import pandas as pd

from database.connection import get_connection
from modules.math_expectation import score_opportunity
import config

logger = logging.getLogger(__name__)


def build_daily_ranking(decisions: list, regime: dict) -> list:
    """
    Build ranked list from scan decisions.
    Filters out ignorar and returns sorted by composite_score.
    """
    rows = []
    for d in decisions:
        action = d.get("action", "ignorar")
        if action == "ignorar":
            continue

        prob     = d.get("prob_success") or 0
        ev_r     = d.get("expected_value_r") or 0
        payoff   = d.get("payoff") or 1
        fund_s   = d.get("fundamental_score") or 0.5
        opp_s    = d.get("opportunity_score") or 0

        # Composite score
        composite = (
            opp_s       * 0.40 +
            ev_r        * 15   * 0.25 +
            prob        * 100  * 0.20 +
            fund_s      * 100  * 0.15
        )

        rows.append({
            "ticker":           d["ticker"],
            "action":           action,
            "action_label":     d.get("action_label", action),
            "setup_name":       d.get("setup_name", ""),
            "composite_score":  round(composite, 2),
            "opportunity_score":round(opp_s, 1),
            "probability":      round(prob, 4),
            "expected_value_r": round(ev_r, 3),
            "payoff":           round(payoff, 2),
            "fundamental_score":round(fund_s, 3),
            "fundamental_grade":d.get("fundamental_grade", "N/A"),
            "entry_price":      d.get("entry_price"),
            "stop_price":       d.get("stop_price"),
            "target_price":     d.get("target_price"),
            "risk_pct":         d.get("risk_pct"),
            "position_pct":     d.get("position_pct"),
            "justification":    d.get("justification", ""),
        })

    # Sort descending by composite score
    rows.sort(key=lambda x: x["composite_score"], reverse=True)

    # Add rank position
    for i, row in enumerate(rows, 1):
        row["rank_position"] = i

    # Persist to DB
    _store_ranking(rows, regime)

    return rows


def _store_ranking(rows: list, regime: dict):
    """Store ranking in DB."""
    today = datetime.today().strftime("%Y-%m-%d")
    conn  = get_connection()
    try:
        for row in rows:
            conn.execute(
                """INSERT OR REPLACE INTO daily_ranking
                   (date, ticker, rank_position, signal_score, fundamental_score,
                    technical_score, expected_value, probability, risk_reward,
                    regime_score, composite_score, action)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    today,
                    row["ticker"],
                    row["rank_position"],
                    row.get("opportunity_score"),
                    row.get("fundamental_score"),
                    row.get("opportunity_score"),   # same for now
                    row.get("expected_value_r"),
                    row.get("probability"),
                    row.get("payoff"),
                    regime.get("aggression", 0.7) * 100,
                    row.get("composite_score"),
                    row.get("action"),
                )
            )
        conn.commit()
    except Exception as e:
        logger.error(f"Store ranking error: {e}")
    finally:
        conn.close()


def get_today_ranking(limit: int = 20) -> list:
    """Return today's ranking from DB."""
    today = datetime.today().strftime("%Y-%m-%d")
    conn  = get_connection()
    try:
        rows = conn.execute(
            """SELECT * FROM daily_ranking
               WHERE date=? ORDER BY composite_score DESC LIMIT ?""",
            (today, limit)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_ranking_by_action(action: str, limit: int = 10) -> list:
    """Filter ranking by action type."""
    today = datetime.today().strftime("%Y-%m-%d")
    conn  = get_connection()
    try:
        rows = conn.execute(
            """SELECT * FROM daily_ranking
               WHERE date=? AND action=?
               ORDER BY composite_score DESC LIMIT ?""",
            (today, action, limit)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()
