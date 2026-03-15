"""
SwingB3 — Flask Application
Motor de Análise e Recomendação de Investimentos para a B3.
"""

import logging
import sys
import os
import threading
from datetime import datetime

from flask import Flask, render_template, request, jsonify, redirect, url_for, flash

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler("logs/app.log", mode="a"),
    ]
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
app.secret_key = "swing-b3-2024-secret"

# ─── Background task state ────────────────────────────────────────────────────
_scan_state   = {"running": False, "done": False, "progress": 0, "total": 0, "result": None, "error": None}
_update_state = {"running": False, "done": False, "result": None, "error": None}

# ─── Initialize DB on startup ─────────────────────────────────────────────────
from database.connection import init_db
from modules.universe import seed_assets

with app.app_context():
    try:
        init_db()
        seed_assets()
        logger.info("DB initialized and assets seeded.")
    except Exception as e:
        logger.error(f"Init error: {e}")


# ─── Context Processors ───────────────────────────────────────────────────────

@app.context_processor
def inject_globals():
    from modules.alerts import get_unread_alerts
    from modules.portfolio import get_user_profile
    try:
        unread = len(get_unread_alerts())
        profile = get_user_profile()
    except Exception:
        unread  = 0
        profile = {}

    def action_badge(action: str) -> str:
        mapping = {
            "comprar_normal":  "bg-success",
            "comprar_pequeno": "bg-success bg-opacity-75",
            "pre_compra":      "bg-info text-dark",
            "manter":          "bg-primary",
            "observar":        "bg-secondary",
            "reduzir":         "bg-warning text-dark",
            "sair":            "bg-danger",
            "ignorar":         "bg-dark",
        }
        return mapping.get(action, "bg-secondary")

    return {
        "unread_alerts": unread,
        "today": datetime.today().strftime("%d/%m/%Y"),
        "user_profile": profile,
        "action_badge": action_badge,
    }


# ─── Dashboard ────────────────────────────────────────────────────────────────

@app.route("/")
def dashboard():
    from modules.market_regime import get_current_regime, get_regime_history
    from modules.ranking import get_today_ranking
    from modules.alerts import get_unread_alerts
    from modules.portfolio import get_portfolio_summary

    try:
        regime    = get_current_regime()
        ranking   = get_today_ranking(10)
        alerts    = get_unread_alerts(5)
        portfolio = get_portfolio_summary()
    except Exception as e:
        logger.error(f"Dashboard error: {e}")
        regime    = {"regime": "unknown", "label": "Calculando..."}
        ranking   = []
        alerts    = []
        portfolio = {}

    return render_template("dashboard.html",
                            regime=regime,
                            ranking=ranking,
                            alerts=alerts,
                            portfolio=portfolio)


# ─── Radar / Scan ─────────────────────────────────────────────────────────────

@app.route("/radar")
def radar():
    from modules.ranking import get_today_ranking
    from modules.market_regime import get_current_regime
    ranking = get_today_ranking(50)
    regime  = get_current_regime()
    return render_template("radar.html", ranking=ranking, regime=regime)


@app.route("/api/scan", methods=["POST"])
def api_scan():
    """Trigger a full market scan (runs in background thread)."""
    if _scan_state["running"]:
        return jsonify({"status": "running", "progress": _scan_state["progress"], "total": _scan_state["total"]})

    def _run():
        _scan_state.update({"running": True, "done": False, "progress": 0, "total": 0, "result": None, "error": None})
        try:
            from modules.decision_engine import run_full_scan
            from modules.ranking import build_daily_ranking
            from modules.market_regime import get_current_regime
            from modules.alerts import check_portfolio_alerts, check_regime_change

            regime    = get_current_regime()
            decisions = run_full_scan(status_dict=_scan_state)
            ranking   = build_daily_ranking(decisions, regime)
            check_portfolio_alerts(decisions)
            check_regime_change(regime.get("regime"))

            buy_actions = ["comprar_pequeno", "comprar_normal", "pre_compra"]
            summary = {
                "total_analyzed": len(decisions),
                "opportunities":  sum(1 for d in decisions if d.get("action") in buy_actions),
                "regime":         regime.get("label"),
                "ranking_count":  len(ranking),
            }
            _scan_state.update({"running": False, "done": True, "result": summary})
        except Exception as e:
            logger.error(f"Scan error: {e}")
            _scan_state.update({"running": False, "done": True, "error": str(e)})

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/scan/status")
def api_scan_status():
    """Return current scan progress."""
    return jsonify(_scan_state)


# ─── Ranking ──────────────────────────────────────────────────────────────────

@app.route("/ranking")
def ranking():
    from modules.ranking import get_today_ranking
    from modules.market_regime import get_current_regime
    full_ranking = get_today_ranking(50)
    regime       = get_current_regime()
    return render_template("ranking.html", ranking=full_ranking, regime=regime)


# ─── Asset Detail ─────────────────────────────────────────────────────────────

@app.route("/asset/<ticker>")
def asset_detail(ticker):
    from modules.decision_engine import analyze_ticker
    from modules.market_regime import get_current_regime
    from modules.data_collector import get_ohlcv_df
    from modules.technical_analysis import get_latest_indicators
    from modules.fundamental_filter import evaluate_fundamental
    from modules.universe import get_asset_info

    ticker  = ticker.upper()
    regime  = get_current_regime()
    info    = get_asset_info(ticker)

    try:
        decision = analyze_ticker(ticker, regime)
        ind      = get_latest_indicators(ticker)
        fund     = evaluate_fundamental(ticker)
        df_ohlcv = get_ohlcv_df(ticker, days=120)
        chart_data = _ohlcv_to_chart(df_ohlcv, ind)
    except Exception as e:
        logger.error(f"Asset detail error {ticker}: {e}")
        decision   = {"action": "observar", "justification": str(e)}
        ind        = {}
        fund       = {}
        chart_data = {}

    return render_template("asset_detail.html",
                            ticker=ticker,
                            info=info,
                            decision=decision,
                            indicators=ind,
                            fundamental=fund,
                            chart_data=chart_data,
                            regime=regime)


def _ohlcv_to_chart(df, ind: dict) -> dict:
    """Prepare chart data for frontend — inclui séries temporais para BB, MACD, EMAs."""
    if df.empty:
        return {}

    from ta import trend as ta_trend, volatility as ta_vol

    def _to_list(series):
        return [None if (v != v) else round(float(v), 4) for v in series]

    dates  = df["date"].astype(str).tolist()
    closes = df["close"].round(2).tolist()
    vols   = df["volume"].fillna(0).astype(int).tolist()

    c = df["close"]

    # Médias móveis
    sma20 = _to_list(ta_trend.sma_indicator(c, window=20, fillna=False))
    sma50 = _to_list(ta_trend.sma_indicator(c, window=50, fillna=False))
    ema9  = _to_list(ta_trend.ema_indicator(c, window=9,  fillna=False))
    ema21 = _to_list(ta_trend.ema_indicator(c, window=21, fillna=False))

    # Bollinger Bands
    bb = ta_vol.BollingerBands(c, window=20, window_dev=2, fillna=False)
    bb_upper = _to_list(bb.bollinger_hband())
    bb_lower = _to_list(bb.bollinger_lband())

    # MACD
    macd_obj  = ta_trend.MACD(c, window_slow=26, window_fast=12, window_sign=9, fillna=False)
    macd_line = _to_list(macd_obj.macd())
    macd_sig  = _to_list(macd_obj.macd_signal())
    macd_hist = _to_list(macd_obj.macd_diff())

    return {
        "dates":     dates,
        "closes":    closes,
        "volumes":   vols,
        "sma20":     sma20,
        "sma50":     sma50,
        "ema9":      ema9,
        "ema21":     ema21,
        "bb_upper":  bb_upper,
        "bb_lower":  bb_lower,
        "macd":      macd_line,
        "macd_sig":  macd_sig,
        "macd_hist": macd_hist,
    }


# ─── Portfolio ────────────────────────────────────────────────────────────────

@app.route("/portfolio")
def portfolio():
    from modules.portfolio import get_portfolio_summary
    summary = get_portfolio_summary()
    return render_template("portfolio.html", summary=summary)


@app.route("/portfolio/add", methods=["POST"])
def portfolio_add():
    from modules.portfolio import add_position
    from modules.universe import get_all_tickers

    data = request.form
    try:
        ticker     = data.get("ticker", "").upper().strip()
        quantity   = float(data.get("quantity", 0))
        avg_price  = float(data.get("avg_price", 0))
        stop_price = float(data.get("stop_price", 0)) or None
        target     = float(data.get("target_price", 0)) or None
        setup      = data.get("setup_name", "manual")
        notes      = data.get("notes", "")

        if not ticker or quantity <= 0 or avg_price <= 0:
            flash("Dados inválidos para adicionar posição.", "danger")
            return redirect(url_for("portfolio"))

        add_position(ticker, quantity, avg_price, stop_price, target, setup, notes)
        flash(f"Posição em {ticker} adicionada com sucesso.", "success")
    except Exception as e:
        flash(f"Erro ao adicionar posição: {e}", "danger")

    return redirect(url_for("portfolio"))


@app.route("/portfolio/close/<int:pos_id>", methods=["POST"])
def portfolio_close(pos_id):
    from modules.portfolio import close_position
    close_price = float(request.form.get("close_price", 0))
    if close_price > 0:
        result = close_position(pos_id, close_price)
        pnl = result.get("pnl", 0)
        pnl_pct = result.get("pnl_pct", 0)
        color = "success" if pnl >= 0 else "danger"
        flash(f"Posição encerrada. PnL: R$ {pnl:.2f} ({pnl_pct:.1f}%)", color)
    return redirect(url_for("portfolio"))


# ─── Recommendations ──────────────────────────────────────────────────────────

@app.route("/recommendations")
def recommendations():
    from modules.ranking import get_ranking_by_action
    from modules.market_regime import get_current_regime

    regime   = get_current_regime()
    buys     = get_ranking_by_action("comprar_normal", 10)
    small    = get_ranking_by_action("comprar_pequeno", 5)
    pre      = get_ranking_by_action("pre_compra", 5)
    maintain = get_ranking_by_action("manter", 10)
    reduce   = get_ranking_by_action("reduzir", 5)
    exit_pos = get_ranking_by_action("sair", 5)

    return render_template("recommendations.html",
                            regime=regime,
                            buys=buys,
                            small_buys=small,
                            pre_buys=pre,
                            maintains=maintain,
                            reduces=reduce,
                            exits=exit_pos)


# ─── Alerts ───────────────────────────────────────────────────────────────────

@app.route("/alerts")
def alerts():
    from modules.alerts import get_all_alerts
    all_alerts = get_all_alerts(100)
    return render_template("alerts.html", alerts=all_alerts)


@app.route("/api/alerts/read/<int:alert_id>", methods=["POST"])
def mark_alert_read(alert_id):
    from modules.alerts import mark_read
    mark_read(alert_id)
    return jsonify({"status": "ok"})


@app.route("/api/alerts/read_all", methods=["POST"])
def mark_alerts_read():
    from modules.alerts import mark_all_read
    mark_all_read()
    return jsonify({"status": "ok"})


# ─── Performance History ──────────────────────────────────────────────────────

@app.route("/history")
def history():
    conn = __import__("database.connection", fromlist=["get_connection"]).get_connection()
    decisions = conn.execute(
        "SELECT * FROM decisions ORDER BY date DESC LIMIT 100"
    ).fetchall()
    closed    = conn.execute(
        """SELECT * FROM portfolio WHERE status='closed'
           ORDER BY closed_at DESC LIMIT 50"""
    ).fetchall()
    conn.close()
    return render_template("history.html",
                            decisions=[dict(d) for d in decisions],
                            closed=[dict(c) for c in closed])


# ─── Profile / Onboarding ────────────────────────────────────────────────────

@app.route("/profile", methods=["GET", "POST"])
def profile():
    from modules.portfolio import get_user_profile, save_user_profile

    if request.method == "POST":
        data = request.form
        profile_data = {
            "total_capital":        float(data.get("total_capital", 100000)),
            "free_capital":         float(data.get("free_capital", 100000)),
            "horizon_days":         int(data.get("horizon_days", 90)),
            "risk_per_trade_pct":   float(data.get("risk_per_trade_pct", 1.0)),
            "max_loss_pct":         float(data.get("max_loss_pct", 10.0)),
            "max_positions":        int(data.get("max_positions", 10)),
            "forbidden_sectors":    data.get("forbidden_sectors", ""),
            "volatility_tolerance": data.get("volatility_tolerance", "medium"),
            "preferred_setups":     data.get("preferred_setups", "all"),
            "strategy":             data.get("strategy", "mixed"),
        }
        save_user_profile(profile_data)
        flash("Perfil atualizado com sucesso!", "success")
        return redirect(url_for("profile"))

    current = get_user_profile()
    return render_template("profile.html", profile=current)


# ─── Data Update ──────────────────────────────────────────────────────────────

@app.route("/api/update", methods=["POST"])
def api_update():
    """Trigger data update (runs in background thread)."""
    if _update_state["running"]:
        return jsonify({"status": "running"})

    req_json = request.json or {}

    def _run():
        _update_state.update({"running": True, "done": False, "result": None, "error": None})
        try:
            from modules.data_collector import run_daily_update
            from modules.universe import get_all_tickers

            tickers = req_json.get("tickers") or get_all_tickers()[:30]
            result = run_daily_update(tickers)
            _update_state.update({"running": False, "done": True, "result": result})
        except Exception as e:
            logger.error(f"Update error: {e}")
            _update_state.update({"running": False, "done": True, "error": str(e)})

    threading.Thread(target=_run, daemon=True).start()
    return jsonify({"status": "started"})


@app.route("/api/update/status")
def api_update_status():
    """Return current update progress."""
    return jsonify(_update_state)


@app.route("/api/update/ticker/<ticker>", methods=["POST"])
def api_update_ticker(ticker):
    """Update a single ticker."""
    from modules.data_collector import update_ohlcv, bulk_update_fundamentals
    from modules.technical_analysis import compute_and_store_indicators
    ticker = ticker.upper()
    try:
        n = update_ohlcv(ticker)
        compute_and_store_indicators(ticker)
        bulk_update_fundamentals([ticker], delay=0)
        return jsonify({"status": "ok", "rows": n, "ticker": ticker})
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500


# ─── Backtest ────────────────────────────────────────────────────────────────

@app.route("/backtest")
def backtest():
    from modules.backtest import get_backtest_results
    results = get_backtest_results()
    return render_template("backtest.html", results=results)


@app.route("/api/backtest/run", methods=["POST"])
def api_run_backtest():
    from modules.backtest import run_all_backtests
    data     = request.json or {}
    tickers  = data.get("tickers", ["PETR4", "VALE3", "ITUB4", "BBDC4", "ABEV3"])
    start    = data.get("start", "2021-01-01")
    end      = data.get("end", datetime.today().strftime("%Y-%m-%d"))

    KEEP = {"ticker","start","end","total_trades","wins","losses","win_rate",
             "avg_gain_pct","avg_loss_pct","payoff","expected_value",
             "total_return","max_drawdown","sharpe_ratio","avg_duration",
             "trades","equity_curve"}
    try:
        results = run_all_backtests(tickers, start, end)
        return jsonify({"status": "ok", "results": {
            k: {kk: vv for kk, vv in v.items() if kk in KEEP}
            for k, v in results.items()
        }})
    except Exception as e:
        logger.error(f"Backtest error: {e}")
        return jsonify({"status": "error", "message": str(e)}), 500


# ─── API: Asset Analysis ──────────────────────────────────────────────────────

@app.route("/api/analyze/<ticker>")
def api_analyze(ticker):
    from modules.decision_engine import analyze_ticker
    from modules.market_regime import get_current_regime
    ticker  = ticker.upper()
    regime  = get_current_regime()
    try:
        result = analyze_ticker(ticker, regime)
        # Remove non-serializable
        result.pop("all_setups", None)
        return jsonify(result)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route("/api/regime")
def api_regime():
    from modules.market_regime import get_current_regime
    return jsonify(get_current_regime())


# ─── Main ────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=5050)
