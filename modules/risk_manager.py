"""
Módulo 10 — Gerenciamento de Risco
Calcula risco por operação, risco da carteira, correlações e limites.
"""

import logging
import numpy as np
import pandas as pd

from database.connection import get_connection
from modules.data_collector import get_ohlcv_df
from modules.technical_analysis import get_latest_indicators
import config

logger = logging.getLogger(__name__)


def calculate_operation_risk(
    ticker: str,
    entry_price: float,
    stop_price: float,
    target_price: float,
    quantity: int,
    portfolio_value: float,
) -> dict:
    """
    Full risk calculation for a single operation.
    """
    if entry_price <= 0 or stop_price <= 0 or quantity <= 0:
        return {"error": "Invalid prices or quantity"}

    risk_per_share = entry_price - stop_price
    if risk_per_share <= 0:
        return {"error": "Stop must be below entry"}

    total_exposure   = entry_price * quantity
    risk_monetary    = risk_per_share * quantity
    risk_pct         = (risk_monetary / portfolio_value) * 100 if portfolio_value > 0 else 0
    gain_per_share   = target_price - entry_price
    rr_ratio         = gain_per_share / risk_per_share if risk_per_share > 0 else 0
    distance_stop    = (risk_per_share / entry_price) * 100

    # ATR-based stop adequacy check
    ind = get_latest_indicators(ticker)
    atr = ind.get("atr14")
    atr_stop_ok = True
    if atr:
        atr_stop_ok = risk_per_share >= float(atr) * 0.5  # stop not too tight

    return {
        "ticker":           ticker,
        "entry_price":      round(entry_price, 2),
        "stop_price":       round(stop_price, 2),
        "target_price":     round(target_price, 2),
        "quantity":         quantity,
        "risk_per_share":   round(risk_per_share, 2),
        "risk_monetary":    round(risk_monetary, 2),
        "risk_pct":         round(risk_pct, 3),
        "total_exposure":   round(total_exposure, 2),
        "rr_ratio":         round(rr_ratio, 2),
        "distance_stop_pct":round(distance_stop, 2),
        "atr":              round(float(atr), 2) if atr else None,
        "atr_stop_adequate":atr_stop_ok,
        "risk_ok":          risk_pct <= config.RISK_PER_TRADE_PCT * 1.5,  # 50% tolerance
    }


def calculate_portfolio_risk(positions: list, portfolio_value: float) -> dict:
    """
    Calculate aggregate portfolio risk metrics.
    positions: list of dicts with ticker, quantity, avg_price, stop_price
    """
    if not positions:
        return {
            "total_risk_monetary": 0,
            "total_risk_pct":      0,
            "sector_exposure":     {},
            "n_positions":         0,
            "portfolio_value":     portfolio_value,
        }

    conn = get_connection()
    total_risk_mon  = 0.0
    sector_exposure = {}
    asset_exposure  = {}

    for pos in positions:
        ticker    = pos.get("ticker")
        qty       = pos.get("quantity", 0)
        stop      = pos.get("stop_price")
        current   = pos.get("current_price") or pos.get("avg_price", 0)

        if stop and current and qty:
            risk_mon = (current - stop) * qty
            total_risk_mon += max(risk_mon, 0)

        # Sector exposure
        row = conn.execute(
            "SELECT sector FROM assets WHERE ticker=?", (ticker,)
        ).fetchone()
        sector = row["sector"] if row else "Outros"
        position_value = (current or 0) * (qty or 0)
        sector_exposure[sector] = sector_exposure.get(sector, 0) + position_value
        asset_exposure[ticker]  = position_value

    conn.close()

    total_risk_pct    = (total_risk_mon / portfolio_value * 100) if portfolio_value > 0 else 0
    sector_pct        = {k: round(v / portfolio_value * 100, 2) for k, v in sector_exposure.items()}
    asset_pct         = {k: round(v / portfolio_value * 100, 2) for k, v in asset_exposure.items()}

    # Largest concentration
    max_sector = max(sector_pct.values()) if sector_pct else 0
    max_asset  = max(asset_pct.values())  if asset_pct  else 0

    alerts = []
    if total_risk_pct > config.MAX_PORTFOLIO_RISK_PCT:
        alerts.append(f"Risco total da carteira ({total_risk_pct:.1f}%) acima do limite ({config.MAX_PORTFOLIO_RISK_PCT}%)")
    if max_sector > config.MAX_SECTOR_EXPOSURE_PCT:
        alerts.append(f"Concentração setorial alta: {max_sector:.1f}%")
    if max_asset > config.MAX_SINGLE_ASSET_PCT:
        alerts.append(f"Concentração por ativo alta: {max_asset:.1f}%")

    return {
        "total_risk_monetary": round(total_risk_mon, 2),
        "total_risk_pct":      round(total_risk_pct, 3),
        "n_positions":         len(positions),
        "sector_exposure_pct": sector_pct,
        "asset_exposure_pct":  asset_pct,
        "max_sector_pct":      max_sector,
        "max_asset_pct":       max_asset,
        "portfolio_value":     portfolio_value,
        "alerts":              alerts,
        "risk_ok":             total_risk_pct <= config.MAX_PORTFOLIO_RISK_PCT,
    }


def incremental_risk(
    ticker: str,
    new_position_value: float,
    new_risk_monetary: float,
    current_portfolio: dict,
    portfolio_value: float,
) -> dict:
    """
    Assess risk impact of adding a new position.
    """
    curr_total_risk = current_portfolio.get("total_risk_monetary", 0)
    curr_sector_exp = current_portfolio.get("sector_exposure_pct", {})
    curr_n_pos      = current_portfolio.get("n_positions", 0)

    # Get sector of new ticker
    conn = get_connection()
    row = conn.execute("SELECT sector FROM assets WHERE ticker=?", (ticker,)).fetchone()
    conn.close()
    new_sector = row["sector"] if row else "Outros"

    new_total_risk_pct = ((curr_total_risk + new_risk_monetary) / portfolio_value * 100)
    new_sector_pct     = curr_sector_exp.get(new_sector, 0) + (new_position_value / portfolio_value * 100)
    new_asset_pct      = new_position_value / portfolio_value * 100

    violations = []
    if new_total_risk_pct > config.MAX_PORTFOLIO_RISK_PCT:
        violations.append(f"Risco total excederia {config.MAX_PORTFOLIO_RISK_PCT}%: {new_total_risk_pct:.1f}%")
    if new_sector_pct > config.MAX_SECTOR_EXPOSURE_PCT:
        violations.append(f"Exposição no setor '{new_sector}' excederia {config.MAX_SECTOR_EXPOSURE_PCT}%: {new_sector_pct:.1f}%")
    if new_asset_pct > config.MAX_SINGLE_ASSET_PCT:
        violations.append(f"Exposição no ativo excederia {config.MAX_SINGLE_ASSET_PCT}%: {new_asset_pct:.1f}%")
    if curr_n_pos >= config.MAX_POSITIONS:
        violations.append(f"Número máximo de posições atingido: {config.MAX_POSITIONS}")

    return {
        "ticker":               ticker,
        "new_total_risk_pct":   round(new_total_risk_pct, 3),
        "new_sector_pct":       round(new_sector_pct, 2),
        "new_asset_pct":        round(new_asset_pct, 2),
        "sector":               new_sector,
        "violations":           violations,
        "approved":             len(violations) == 0,
    }


def calculate_correlation_risk(ticker: str, existing_tickers: list,
                                 lookback: int = 63) -> float:
    """
    Compute average correlation of new ticker with existing positions.
    Returns average correlation (0 = independent, 1 = fully correlated).
    """
    if not existing_tickers:
        return 0.0

    try:
        all_tickers = [ticker] + existing_tickers
        returns = {}
        for t in all_tickers:
            df = get_ohlcv_df(t, days=lookback + 10)
            if not df.empty and len(df) >= lookback:
                returns[t] = df["close"].pct_change().tail(lookback).values

        if len(returns) < 2:
            return 0.0

        ret_df = pd.DataFrame(returns).dropna()
        corr_matrix = ret_df.corr()

        # Average correlation of new ticker with all existing
        avg_corr = corr_matrix[ticker].drop(ticker).abs().mean()
        return round(float(avg_corr), 4)

    except Exception as e:
        logger.debug(f"Correlation error: {e}")
        return 0.0


def get_stop_suggestion(ticker: str, entry_price: float,
                          method: str = "atr") -> float:
    """
    Suggest stop price using ATR or recent low.
    method: 'atr' | 'recent_low' | 'sma'
    """
    ind = get_latest_indicators(ticker)

    if method == "atr":
        atr = ind.get("atr14")
        if atr:
            return round(entry_price - float(atr) * 1.5, 2)

    if method == "sma":
        sma20 = ind.get("sma20")
        if sma20:
            return round(float(sma20) * 0.98, 2)

    # Fallback: recent low
    df = get_ohlcv_df(ticker, days=20)
    if not df.empty:
        return round(float(df["low"].tail(10).min()) * 0.995, 2)

    return round(entry_price * 0.95, 2)
