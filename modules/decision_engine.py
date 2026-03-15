"""
Módulo 15 — Motor de Decisão
Combina todos os módulos e gera decisão final por ativo.
"""

import logging
from datetime import datetime

from database.connection import get_connection
from modules.fundamental_filter import evaluate_fundamental
from modules.setups import detect_all_setups, get_setup_stats
from modules.market_regime import get_current_regime
from modules.probability import calculate_probability
from modules.risk_manager import (
    calculate_operation_risk, incremental_risk,
    calculate_correlation_risk, get_stop_suggestion
)
from modules.return_estimator import estimate_return, calculate_technical_target
from modules.math_expectation import calculate_expected_value, score_opportunity
from modules.position_sizing import calculate_position_size, suggest_size_label
from modules.portfolio import get_portfolio, get_user_profile, get_open_tickers
import config

logger = logging.getLogger(__name__)


def analyze_ticker(ticker: str, regime: dict = None) -> dict:
    """
    Full analysis pipeline for a single ticker.
    Returns complete decision dict.
    """
    if regime is None:
        regime = get_current_regime()

    profile  = get_user_profile()
    port     = get_portfolio()
    port_val = profile.get("total_capital", 100000)
    free_cap = profile.get("free_capital", 100000)
    open_tickers = get_open_tickers()

    # ── 1. Fundamental Filter ─────────────────────────────────────────────────
    fund = evaluate_fundamental(ticker)
    fund_grade = fund.get("fundamental_grade", "C")
    fund_class = fund.get("classification", "neutro")

    if fund_class == "reprovado":
        return _make_decision(
            ticker, "ignorar",
            justification=f"Reprovado no filtro fundamentalista: {'; '.join(fund.get('disqualifiers', ['qualidade ruim']))}",
            fundamental=fund,
        )

    # ── 2. Detect Setups ──────────────────────────────────────────────────────
    setups = detect_all_setups(ticker)

    if not setups:
        return _make_decision(
            ticker, "observar",
            justification="Sem setup técnico detectado no momento.",
            fundamental=fund,
        )

    # Pick best setup (highest confidence)
    setup = max(setups, key=lambda s: s.get("confidence", 0))
    setup_name = setup.get("setup")
    entry  = setup.get("entry")
    stop   = setup.get("stop")
    target = setup.get("target")

    if not entry or not stop:
        return _make_decision(
            ticker, "observar",
            justification="Setup detectado mas sem nível de entrada/stop definido.",
            fundamental=fund, setups=setups,
        )

    # ── 3. Probability ────────────────────────────────────────────────────────
    prob_result = calculate_probability(setup, ticker, regime, fund_grade)
    prob        = prob_result.get("prob_success", 0.50)

    # ── 4. Return / Risk ──────────────────────────────────────────────────────
    stats     = get_setup_stats(setup_name, regime.get("regime"))
    avg_gain  = stats.get("avg_gain_pct", 5.0)
    avg_loss  = stats.get("avg_loss_pct", -3.0)

    # Target: use setup target or calculate technical target
    if not target or target <= entry:
        target = calculate_technical_target(ticker, entry, setup_name, stop)

    # ── 5. Mathematical Expectation ───────────────────────────────────────────
    ev = calculate_expected_value(prob, avg_gain, abs(avg_loss))
    ev_r   = ev.get("expected_value_r", 0)
    payoff = ev.get("payoff", 2.0)

    if not ev.get("is_tradeable"):
        action = "observar"
        just = (f"Expectativa matemática insuficiente: EV={ev_r:.2f}R. "
                f"Prob={prob:.0%}, R:R={payoff:.1f}:1")
        return _make_decision(
            ticker, action, justification=just,
            fundamental=fund, setups=setups,
            probability=prob_result, ev=ev,
        )

    # ── 6. Position Sizing ────────────────────────────────────────────────────
    corr = calculate_correlation_risk(ticker, open_tickers)
    sizing = calculate_position_size(
        ticker=ticker,
        entry_price=entry,
        stop_price=stop,
        portfolio_value=port_val,
        free_capital=free_cap,
        regime_aggression=regime.get("aggression", 1.0),
        correlation=corr,
        prob_success=prob,
        payoff=payoff,
    )

    quantity = sizing.get("quantity", 0)
    if quantity <= 0 or not sizing.get("feasible"):
        return _make_decision(
            ticker, "observar",
            justification="Capital insuficiente para posição mínima.",
            fundamental=fund, setups=setups, sizing=sizing,
        )

    # ── 7. Portfolio Risk Check ────────────────────────────────────────────────
    risk_op = calculate_operation_risk(
        ticker, entry, stop, target, quantity, port_val
    )
    port_risk_data = {"total_risk_monetary": sum(
        (p.get("current_price", p["avg_price"]) - (p.get("stop_price") or p["avg_price"] * 0.95)) * p["quantity"]
        for p in port
    ), "sector_exposure_pct": {}, "n_positions": len(port)}

    inc_risk = incremental_risk(
        ticker,
        new_position_value=sizing.get("capital_required", entry * quantity),
        new_risk_monetary=sizing.get("risk_monetary", 0),
        current_portfolio=port_risk_data,
        portfolio_value=port_val,
    )

    if not inc_risk.get("approved"):
        violations = inc_risk.get("violations", [])
        return _make_decision(
            ticker, "observar",
            justification=f"Limite de carteira: {'; '.join(violations)}",
            fundamental=fund, setups=setups, sizing=sizing, ev=ev,
        )

    # ── 8. Already in Portfolio? ──────────────────────────────────────────────
    if ticker in open_tickers:
        pos = next((p for p in port if p["ticker"] == ticker), None)
        if pos:
            curr = pos.get("current_price") or pos["avg_price"]
            if pos.get("stop_price") and curr <= pos["stop_price"]:
                action = "sair"
                just   = f"Stop rompido: preço {curr:.2f} <= stop {pos['stop_price']:.2f}"
            elif pos.get("target_price") and curr >= pos["target_price"] * 0.97:
                action = "reduzir"
                just   = f"Próximo ao alvo: {curr:.2f} / {pos['target_price']:.2f}"
            else:
                action = "manter"
                just   = f"Posição aberta saudável. PnL: {(curr - pos['avg_price']) / pos['avg_price'] * 100:.1f}%"

            return _make_decision(
                ticker, action, justification=just,
                fundamental=fund, setups=setups,
                probability=prob_result, ev=ev, sizing=sizing,
                stop=stop, target=target,
            )

    # ── 9. Final Buy Decision ─────────────────────────────────────────────────
    size_label = suggest_size_label(sizing.get("position_pct", 5))
    action     = size_label

    # Extra conditions to downgrade
    if prob < 0.52 or ev_r < 0.30:
        action = "pre_compra"

    opp_score = score_opportunity(ev, setup, regime)

    just_parts = [
        f"Setup: {setup.get('name')}",
        f"Prob: {prob:.0%}",
        f"EV: {ev_r:.2f}R",
        f"R:R: {payoff:.1f}:1",
        f"Fundamentos: {fund_grade}",
        f"Regime: {regime.get('label', '')}",
        f"Score: {opp_score:.0f}/100",
    ]
    justification = " | ".join(just_parts)

    decision = _make_decision(
        ticker, action,
        justification=justification,
        fundamental=fund, setups=setups,
        probability=prob_result, ev=ev, sizing=sizing,
        stop=stop, target=target,
        opp_score=opp_score,
    )
    decision["return_estimate"] = estimate_return(
        ticker, entry, stop, target, quantity, setup_name, prob
    )
    _store_decision(decision, regime)
    return decision


def _make_decision(ticker, action, justification="", fundamental=None,
                    setups=None, probability=None, ev=None, sizing=None,
                    stop=None, target=None, opp_score=None) -> dict:
    fund = fundamental or {}
    proba = probability or {}
    evd   = ev or {}
    sz    = sizing or {}
    ss    = (setups[0] if setups else {})

    return {
        "ticker":             ticker,
        "action":             action,
        "action_label":       _action_label(action),
        "justification":      justification,
        "probability":        proba.get("prob_success"),
        "fundamental_grade":  fund.get("fundamental_grade"),
        "fundamental_score":  fund.get("fundamental_score"),
        "expected_value_r":   evd.get("expected_value_r"),
        "expected_value_pct": evd.get("expected_value_pct"),
        "payoff":             evd.get("payoff"),
        "prob_success":       proba.get("prob_success"),
        "prob_failure":       proba.get("prob_failure"),
        "setup":              ss.get("setup"),
        "setup_name":         ss.get("name"),
        "entry_price":        ss.get("entry"),
        "stop_price":         stop or ss.get("stop"),
        "target_price":       target or ss.get("target"),
        "suggested_quantity": sz.get("quantity"),
        "capital_required":   sz.get("capital_required"),
        "position_pct":       sz.get("position_pct"),
        "risk_monetary":      sz.get("risk_monetary"),
        "risk_pct":           sz.get("risk_pct"),
        "opportunity_score":  opp_score,
        "all_setups":         setups or [],
        "fundamental_detail": fund,
        "probability_detail": proba,
        "ev_detail":          evd,
        "sizing_detail":      sz,
    }


def _action_label(action: str) -> str:
    labels = {
        "ignorar":        "Ignorar",
        "observar":       "Observar",
        "pre_compra":     "Pré-Compra",
        "comprar_pequeno":"Comprar Pequeno",
        "comprar_normal": "Comprar",
        "manter":         "Manter",
        "reduzir":        "Reduzir",
        "sair":           "Sair",
    }
    return labels.get(action, action)


def _store_decision(decision: dict, regime: dict):
    """Persist decision to DB."""
    conn = get_connection()
    try:
        today = datetime.today().strftime("%Y-%m-%d")
        conn.execute(
            """INSERT OR REPLACE INTO decisions
               (ticker, date, action, probability, risk_pct, expected_return_pct,
                expected_value, suggested_size_pct, stop_price, target_price,
                setup_name, regime, fundamental_grade, justification)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                decision["ticker"], today, decision["action"],
                decision.get("prob_success"),
                decision.get("risk_pct"),
                decision.get("expected_value_pct"),
                decision.get("expected_value_r"),
                decision.get("position_pct"),
                decision.get("stop_price"),
                decision.get("target_price"),
                decision.get("setup"),
                regime.get("regime"),
                decision.get("fundamental_grade"),
                decision.get("justification"),
            )
        )
        conn.commit()
    except Exception as e:
        logger.error(f"Store decision error: {e}")
    finally:
        conn.close()


def run_full_scan(tickers: list = None) -> list:
    """
    Run analysis on all eligible tickers and return list of decisions.
    """
    from modules.universe import get_eligible_tickers, get_all_tickers
    from modules.data_collector import get_ohlcv_df

    if tickers is None:
        eligible = get_eligible_tickers()
        if not eligible:
            eligible = get_all_tickers()[:50]  # fallback for fresh DB
    else:
        eligible = tickers

    regime    = get_current_regime()
    decisions = []

    logger.info(f"Scanning {len(eligible)} tickers...")

    for ticker in eligible:
        try:
            df = get_ohlcv_df(ticker, days=5)
            if df.empty:
                continue
            decision = analyze_ticker(ticker, regime)
            decisions.append(decision)
        except Exception as e:
            logger.error(f"Scan error {ticker}: {e}")

    logger.info(f"Scan complete: {len(decisions)} analyzed")
    return decisions
