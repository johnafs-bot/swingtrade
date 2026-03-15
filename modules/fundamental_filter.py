"""
Módulo 5 — Filtro Fundamentalista
Avalia qualidade e risco estrutural dos ativos.
NÃO prevê preço — filtra ativos com fundamentos ruins.
"""

import logging
import numpy as np
import pandas as pd
from database.connection import get_connection
import config

logger = logging.getLogger(__name__)

# Grading weights
FUNDAMENTAL_WEIGHTS = {
    "lucratividade": 0.25,
    "endividamento":  0.20,
    "rentabilidade":  0.20,
    "crescimento":    0.15,
    "valuation":      0.15,
    "consistencia":   0.05,
}


def get_latest_fundamentals(ticker: str) -> dict:
    """Load latest fundamental data for a ticker."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM fundamentals WHERE ticker=? ORDER BY date DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def score_lucratividade(data: dict) -> float:
    """Score 0–1 based on net margin and EBITDA margin."""
    nm  = data.get("net_margin")
    em  = data.get("ebitda_margin")
    ni  = data.get("net_income")

    score = 0.0
    count = 0

    if nm is not None:
        if nm > 0.20:     score += 1.0
        elif nm > 0.10:   score += 0.75
        elif nm > 0.05:   score += 0.5
        elif nm > 0.0:    score += 0.25
        else:             score += 0.0
        count += 1

    if em is not None:
        if em > 0.30:     score += 1.0
        elif em > 0.15:   score += 0.75
        elif em > 0.08:   score += 0.5
        elif em > 0.0:    score += 0.25
        else:             score += 0.0
        count += 1

    if ni is not None:
        score += 1.0 if ni > 0 else 0.0
        count += 1

    return score / count if count > 0 else 0.5


def score_endividamento(data: dict) -> float:
    """Score 0–1 based on net debt / EBITDA."""
    nd_ebitda = data.get("net_debt_ebitda")
    net_debt  = data.get("net_debt")

    if nd_ebitda is None:
        if net_debt is not None and net_debt <= 0:
            return 1.0   # net cash
        return 0.5

    if net_debt is not None and net_debt <= 0:
        return 1.0       # net cash position

    if nd_ebitda < 0.5:   return 1.0
    elif nd_ebitda < 1.5: return 0.85
    elif nd_ebitda < 2.5: return 0.65
    elif nd_ebitda < 3.5: return 0.40
    elif nd_ebitda < 5.0: return 0.20
    else:                 return 0.0


def score_rentabilidade(data: dict) -> float:
    """Score 0–1 based on ROE and ROIC."""
    roe  = data.get("roe")
    roic = data.get("roic")

    score = 0.0
    count = 0

    for val in [roe, roic]:
        if val is None:
            continue
        if val > 0.25:    score += 1.0
        elif val > 0.15:  score += 0.80
        elif val > 0.10:  score += 0.60
        elif val > 0.05:  score += 0.35
        elif val > 0.0:   score += 0.15
        else:             score += 0.0
        count += 1

    return score / count if count > 0 else 0.5


def score_crescimento(data: dict) -> float:
    """Score 0–1 based on revenue and income growth."""
    rev_g = data.get("revenue_growth_yoy")
    inc_g = data.get("income_growth_yoy")

    score = 0.0
    count = 0

    for val in [rev_g, inc_g]:
        if val is None:
            continue
        if val > 0.20:    score += 1.0
        elif val > 0.10:  score += 0.80
        elif val > 0.0:   score += 0.60
        elif val > -0.10: score += 0.30
        else:             score += 0.0
        count += 1

    return score / count if count > 0 else 0.5


def score_valuation(data: dict) -> float:
    """Score 0–1 (higher = cheaper valuation)."""
    pl  = data.get("pl_ratio")
    ev  = data.get("ev_ebitda")
    dy  = data.get("dividend_yield")

    score = 0.0
    count = 0

    if pl is not None and pl > 0:
        if pl < 8:         score += 1.0
        elif pl < 15:      score += 0.80
        elif pl < 25:      score += 0.60
        elif pl < 40:      score += 0.35
        elif pl < 60:      score += 0.15
        else:              score += 0.0
        count += 1

    if ev is not None and ev > 0:
        if ev < 5:         score += 1.0
        elif ev < 8:       score += 0.80
        elif ev < 12:      score += 0.60
        elif ev < 18:      score += 0.35
        elif ev < 25:      score += 0.15
        else:              score += 0.0
        count += 1

    if dy is not None:
        if dy > 0.08:      score += 1.0
        elif dy > 0.05:    score += 0.75
        elif dy > 0.02:    score += 0.50
        else:              score += 0.25
        count += 1

    return score / count if count > 0 else 0.5


def score_consistencia(data: dict) -> float:
    """Proxy for consistency — check that key metrics are available and positive."""
    required = ["revenue", "net_income", "ebitda", "roe"]
    present = sum(1 for k in required if data.get(k) is not None)
    return present / len(required)


def evaluate_fundamental(ticker: str) -> dict:
    """
    Full fundamental evaluation.
    Returns dict with scores, grade (A/B/C/D/F), and classification.
    """
    data = get_latest_fundamentals(ticker)
    if not data:
        return {
            "ticker": ticker,
            "fundamental_score": None,
            "fundamental_grade": "N/A",
            "classification": "neutro",
            "detail": "Sem dados fundamentalistas",
            "scores": {},
        }

    scores = {
        "lucratividade": score_lucratividade(data),
        "endividamento":  score_endividamento(data),
        "rentabilidade":  score_rentabilidade(data),
        "crescimento":    score_crescimento(data),
        "valuation":      score_valuation(data),
        "consistencia":   score_consistencia(data),
    }

    # Weighted composite score
    composite = sum(scores[k] * FUNDAMENTAL_WEIGHTS[k] for k in scores)

    # Grade
    if composite >= 0.80:   grade = "A"
    elif composite >= 0.65: grade = "B"
    elif composite >= 0.50: grade = "C"
    elif composite >= 0.35: grade = "D"
    else:                   grade = "F"

    # Classification
    if grade in ("A", "B"):
        classification = "aprovado"
    elif grade == "C":
        classification = "neutro"
    else:
        classification = "reprovado"

    # Hard disqualifiers
    disqualifiers = []
    nd_eb = data.get("net_debt_ebitda")
    net_d = data.get("net_debt", 0) or 0

    if nd_eb is not None and net_d > 0 and nd_eb > config.MAX_NET_DEBT_EBITDA:
        disqualifiers.append(f"Dívida/EBITDA elevada: {nd_eb:.1f}x")

    ni = data.get("net_income")
    if ni is not None and ni < 0:
        disqualifiers.append("Prejuízo líquido")

    if disqualifiers:
        classification = "reprovado"
        grade = min(grade, "D")

    # Update DB
    _update_db_score(ticker, composite, grade)

    return {
        "ticker": ticker,
        "fundamental_score": round(composite, 4),
        "fundamental_grade": grade,
        "classification": classification,
        "disqualifiers": disqualifiers,
        "scores": {k: round(v, 3) for k, v in scores.items()},
        "data": data,
    }


def _update_db_score(ticker: str, score: float, grade: str):
    """Update fundamental_score and grade in fundamentals table."""
    from datetime import datetime
    today = datetime.today().strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        conn.execute(
            """UPDATE fundamentals SET fundamental_score=?, fundamental_grade=?
               WHERE ticker=? AND date=?""",
            (score, grade, ticker, today)
        )
        conn.commit()
    finally:
        conn.close()


def bulk_evaluate(tickers: list) -> dict:
    """Evaluate fundamentals for all tickers, return dict ticker -> result."""
    results = {}
    for ticker in tickers:
        try:
            results[ticker] = evaluate_fundamental(ticker)
        except Exception as e:
            logger.error(f"Fundamental eval error {ticker}: {e}")
            results[ticker] = {
                "ticker": ticker,
                "fundamental_grade": "N/A",
                "classification": "neutro",
                "fundamental_score": None,
            }
    return results


def filter_approved(tickers: list, allow_neutral: bool = True) -> list:
    """
    Return tickers with 'aprovado' or 'neutro' (if allowed) classification.
    """
    results = bulk_evaluate(tickers)
    out = []
    for ticker, res in results.items():
        cls = res.get("classification", "neutro")
        if cls == "aprovado":
            out.append(ticker)
        elif allow_neutral and cls == "neutro":
            out.append(ticker)
    return out
