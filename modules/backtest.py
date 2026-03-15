"""
Módulo 19 — Backtest e Validação
Testa setups sobre histórico, mede taxa de acerto, payoff e drawdown.
Evita overfitting por meio de walk-forward simples.
"""

import logging
import json
from datetime import datetime
from typing import Callable, Optional
import numpy as np
import pandas as pd

from database.connection import get_connection
from modules.data_collector import get_ohlcv_df
from modules.technical_analysis import compute_indicators
import config

logger = logging.getLogger(__name__)


def backtest_setup(
    ticker: str,
    setup_fn: Callable,
    start_date: str,
    end_date: str,
    max_hold_days: int = config.MAX_HORIZON_DAYS,
    setup_params: dict = None,
) -> dict:
    """
    Backtest a setup function on historical data.

    setup_fn: function(df_window) -> dict with 'detected', 'entry', 'stop', 'target'
    Returns dict with performance metrics.
    """
    df_full = get_ohlcv_df(ticker, days=1500)
    if df_full.empty or len(df_full) < 100:
        return {"error": "Insufficient data"}

    # Filter date range
    df_full["date"] = pd.to_datetime(df_full["date"])
    mask = (df_full["date"] >= pd.to_datetime(start_date)) & \
           (df_full["date"] <= pd.to_datetime(end_date))
    df_range = df_full[mask].reset_index(drop=True)

    if len(df_range) < 60:
        return {"error": "Date range too short"}

    # Compute indicators for full range (using all prior data)
    df_ind = compute_indicators(df_full.copy())
    if df_ind.empty:
        return {"error": "Indicator computation failed"}

    trades = []
    i = 60  # start after warmup

    while i < len(df_range) - max_hold_days:
        # Prepare window for setup detection
        window_ohlcv = df_range.iloc[:i+1].copy()
        window_ind   = df_ind.iloc[:i+1].copy() if len(df_ind) > i else df_ind.copy()

        try:
            signal = setup_fn(window_ohlcv, window_ind)
        except Exception:
            i += 1
            continue

        if not signal.get("detected"):
            i += 1
            continue

        # Entry
        entry  = float(signal.get("entry", df_range.iloc[i]["close"]))
        stop   = float(signal.get("stop", entry * 0.95))
        target = float(signal.get("target", entry * 1.05))
        risk   = entry - stop

        if risk <= 0:
            i += 1
            continue

        # Simulate forward
        trade_result = _simulate_trade(
            df_range, i + 1, entry, stop, target, max_hold_days
        )
        trade_result["date_entry"]   = str(df_range.iloc[i]["date"].date())
        trade_result["entry_price"]  = entry
        trade_result["stop_price"]   = stop
        trade_result["target_price"] = target

        trades.append(trade_result)

        # Advance past trade duration
        i += max(trade_result.get("duration", 5), 5)

    if not trades:
        return {
            "ticker": ticker, "start": start_date, "end": end_date,
            "total_trades": 0, "error": "No signals found"
        }

    return _compute_stats(ticker, trades, start_date, end_date, setup_params)


def _simulate_trade(df: pd.DataFrame, start_idx: int,
                     entry: float, stop: float, target: float,
                     max_hold: int) -> dict:
    """Simulate trade outcome with forward price data."""
    end_idx = min(start_idx + max_hold, len(df))

    for j in range(start_idx, end_idx):
        row = df.iloc[j]
        lo  = float(row["low"])
        hi  = float(row["high"])
        cl  = float(row["close"])
        duration = j - start_idx + 1

        # Stop hit (intraday low)
        if lo <= stop:
            loss_pct = (stop - entry) / entry * 100
            return {
                "outcome":    "loss",
                "pnl_pct":    round(loss_pct, 3),
                "exit_price": stop,
                "duration":   duration,
                "exit_reason":"stop",
            }

        # Target hit (intraday high)
        if hi >= target:
            gain_pct = (target - entry) / entry * 100
            return {
                "outcome":    "win",
                "pnl_pct":    round(gain_pct, 3),
                "exit_price": target,
                "duration":   duration,
                "exit_reason":"target",
            }

    # Time exit
    exit_price = float(df.iloc[end_idx - 1]["close"])
    pnl = (exit_price - entry) / entry * 100
    return {
        "outcome":    "win" if pnl > 0 else "loss",
        "pnl_pct":    round(pnl, 3),
        "exit_price": exit_price,
        "duration":   max_hold,
        "exit_reason":"time",
    }


def _compute_stats(ticker: str, trades: list,
                    start: str, end: str, params: dict = None) -> dict:
    """Compute performance statistics from list of trades."""
    total   = len(trades)
    wins    = [t for t in trades if t["outcome"] == "win"]
    losses  = [t for t in trades if t["outcome"] == "loss"]

    win_rate  = len(wins) / total if total > 0 else 0
    avg_gain  = np.mean([t["pnl_pct"] for t in wins])  if wins   else 0
    avg_loss  = np.mean([t["pnl_pct"] for t in losses]) if losses else 0
    payoff    = abs(avg_gain / avg_loss) if avg_loss != 0 else 0
    ev        = win_rate * avg_gain + (1 - win_rate) * avg_loss
    avg_dur   = np.mean([t["duration"] for t in trades])

    # Cumulative return
    equity = 100.0
    equity_curve = [100.0]
    for t in trades:
        equity *= (1 + t["pnl_pct"] / 100)
        equity_curve.append(equity)

    total_return = equity - 100
    max_dd = _max_drawdown(equity_curve)

    # Sharpe (simple)
    returns = [t["pnl_pct"] for t in trades]
    sharpe  = (np.mean(returns) / np.std(returns) * np.sqrt(252 / avg_dur)
               if len(returns) > 1 and np.std(returns) > 0 else 0)

    result = {
        "ticker":        ticker,
        "start":         start,
        "end":           end,
        "total_trades":  total,
        "wins":          len(wins),
        "losses":        len(losses),
        "win_rate":      round(win_rate, 4),
        "avg_gain_pct":  round(avg_gain, 3),
        "avg_loss_pct":  round(avg_loss, 3),
        "payoff":        round(payoff, 3),
        "expected_value":round(ev, 3),
        "total_return":  round(total_return, 2),
        "max_drawdown":  round(max_dd, 2),
        "sharpe_ratio":  round(sharpe, 3),
        "avg_duration":  round(avg_dur, 1),
        "trades":        trades[:50],  # limit for storage
        "equity_curve":  [round(v, 2) for v in equity_curve],
        "params":        params or {},
    }
    return result


def _max_drawdown(equity_curve: list) -> float:
    """Calculate maximum drawdown percentage."""
    if len(equity_curve) < 2:
        return 0
    peak = equity_curve[0]
    max_dd = 0
    for v in equity_curve:
        if v > peak:
            peak = v
        dd = (peak - v) / peak * 100
        max_dd = max(max_dd, dd)
    return max_dd


def save_backtest_result(setup_name: str, result: dict, ticker_filter: str = None):
    """Store backtest result in DB."""
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO backtest_results
               (setup_name, ticker_filter, date_start, date_end, total_trades,
                win_rate, avg_gain, avg_loss, payoff, expected_value,
                max_drawdown, total_return, sharpe_ratio, params)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (
                setup_name,
                ticker_filter,
                result.get("start"),
                result.get("end"),
                result.get("total_trades"),
                result.get("win_rate"),
                result.get("avg_gain_pct"),
                result.get("avg_loss_pct"),
                result.get("payoff"),
                result.get("expected_value"),
                result.get("max_drawdown"),
                result.get("total_return"),
                result.get("sharpe_ratio"),
                json.dumps(result.get("params", {})),
            )
        )
        # Update setup_stats table
        wr     = result.get("win_rate", 0)
        ag     = result.get("avg_gain_pct", 0)
        al     = result.get("avg_loss_pct", 0)
        po     = result.get("payoff", 0)
        ev     = result.get("expected_value", 0)
        dur    = result.get("avg_duration", 15)
        total  = result.get("total_trades", 0)
        w      = result.get("wins", 0)
        l      = result.get("losses", 0)

        conn.execute(
            """INSERT OR REPLACE INTO setup_stats
               (setup_name, regime, total_trades, wins, losses, win_rate,
                avg_gain_pct, avg_loss_pct, payoff, expected_value, avg_duration)
               VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
            (setup_name, "all", total, w, l, wr, ag, al, po, ev, dur)
        )
        conn.commit()
    finally:
        conn.close()


def get_backtest_results(setup_name: str = None) -> list:
    """Retrieve backtest results from DB."""
    conn = get_connection()
    try:
        if setup_name:
            rows = conn.execute(
                "SELECT * FROM backtest_results WHERE setup_name=? ORDER BY created_at DESC",
                (setup_name,)
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT * FROM backtest_results ORDER BY created_at DESC LIMIT 50"
            ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def run_all_backtests(tickers: list = None, start: str = "2020-01-01",
                       end: str = None) -> dict:
    """
    Run backtests for all setups across multiple tickers.
    """
    from modules.setups import (
        detect_breakout_consolidacao, detect_pullback_tendencia,
        detect_continuacao_momentum, detect_reversao_confirmada,
    )

    tickers = tickers or ["PETR4", "VALE3", "ITUB4", "BBDC4", "MGLU3",
                           "ABEV3", "WEGE3", "RENT3", "RAIL3", "SUZB3"]
    end = end or datetime.today().strftime("%Y-%m-%d")

    setups_to_test = {
        "breakout_consolidacao": detect_breakout_consolidacao,
        "pullback_tendencia":    detect_pullback_tendencia,
        "continuacao_momentum":  detect_continuacao_momentum,
        "reversao_confirmada":   detect_reversao_confirmada,
    }

    all_results = {}
    for setup_name, setup_fn in setups_to_test.items():
        setup_results = []
        for ticker in tickers:
            try:
                res = backtest_setup(ticker, setup_fn, start, end)
                if "error" not in res and res.get("total_trades", 0) > 0:
                    setup_results.append(res)
            except Exception as e:
                logger.error(f"Backtest error {setup_name}/{ticker}: {e}")

        if setup_results:
            # Aggregate across tickers
            agg = _aggregate_results(setup_name, setup_results)
            save_backtest_result(setup_name, agg)
            all_results[setup_name] = agg

    return all_results


def _aggregate_results(setup_name: str, results: list) -> dict:
    """Aggregate backtest results across multiple tickers."""
    all_trades  = []
    for r in results:
        all_trades.extend(r.get("trades", []))

    return _compute_stats(setup_name, all_trades,
                           results[0].get("start", ""),
                           results[0].get("end", ""))
