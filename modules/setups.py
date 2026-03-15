"""
Módulo 7 — Setups Operacionais
Define e detecta setups técnicos testáveis em série histórica numérica.
"""

import logging
import numpy as np
import pandas as pd

from database.connection import get_connection
from modules.data_collector import get_ohlcv_df
from modules.technical_analysis import (
    get_indicators_df,
    find_breakout,
    find_consolidation,
    find_pullback_in_trend,
)
import config

logger = logging.getLogger(__name__)


# ─── Setup Registry ───────────────────────────────────────────────────────────

SETUP_DEFINITIONS = {
    "breakout_consolidacao": {
        "name":        "Rompimento de Consolidação",
        "description": "Preço rompe região de consolidação com volume acima da média.",
        "direction":   "long",
        "horizon":     "swing",  # swing | position
    },
    "pullback_tendencia": {
        "name":        "Pullback em Tendência",
        "description": "Correção curta em ativo em tendência de alta, próximo à SMA20.",
        "direction":   "long",
        "horizon":     "swing",
    },
    "continuacao_momentum": {
        "name":        "Continuação de Momentum",
        "description": "Ativo com forte momentum relativo mantendo tendência de alta.",
        "direction":   "long",
        "horizon":     "position",
    },
    "reversao_confirmada": {
        "name":        "Reversão com Confirmação",
        "description": "Ativo sobrevende com divergência e sinal de retomada.",
        "direction":   "long",
        "horizon":     "swing",
    },
    "inside_bar_breakout": {
        "name":        "Inside Bar Rompimento",
        "description": "Candle interno seguido de rompimento direcional.",
        "direction":   "both",
        "horizon":     "swing",
    },
}


# ─── Individual Setup Detection ───────────────────────────────────────────────

def detect_breakout_consolidacao(df_ohlcv: pd.DataFrame,
                                  df_ind: pd.DataFrame,
                                  lookback: int = 20) -> dict:
    """
    Setup: Rompimento de Consolidação
    Critério:
    - Price range last `lookback` bars < 1.5x ATR * lookback (consolidação)
    - Last candle closes above max of prior lookback bars
    - Volume >= 1.5x average
    """
    if len(df_ohlcv) < lookback + 5 or df_ind.empty:
        return _no_setup("breakout_consolidacao")

    merged = df_ohlcv.copy()
    if "atr14" in df_ind.columns:
        merged = merged.merge(df_ind[["date","atr14","rvol","trend_mid","sma20","rsi14"]],
                              on="date", how="left")

    recent   = merged.tail(lookback + 1)
    prev     = recent.iloc[:-1]
    last     = recent.iloc[-1]
    prev_max = prev["high"].max()
    prev_min = prev["low"].min()
    prev_range = prev_max - prev_min
    atr      = last.get("atr14") or last.get("atr14", np.nan)

    if pd.isna(atr) or atr == 0:
        return _no_setup("breakout_consolidacao")

    # Consolidation check
    in_consolidation = (prev_range <= atr * config.CONSOLIDATION_ATR_MULT * lookback)
    breakout_up      = last["close"] > prev_max
    rvol             = last.get("rvol", 1.0) or 1.0
    vol_confirmed    = float(rvol) >= config.BREAKOUT_VOL_MULT

    if in_consolidation and breakout_up and vol_confirmed:
        stop    = float(prev_min)
        risk    = float(last["close"]) - stop
        target  = float(last["close"]) + risk * 2.5  # R:R 2.5
        return {
            "detected": True,
            "setup":    "breakout_consolidacao",
            "name":     SETUP_DEFINITIONS["breakout_consolidacao"]["name"],
            "direction":"long",
            "entry":    float(last["close"]),
            "stop":     round(stop, 2),
            "target":   round(target, 2),
            "rvol":     round(float(rvol), 2),
            "criteria": {
                "in_consolidation": in_consolidation,
                "breakout_up":      breakout_up,
                "vol_confirmed":    vol_confirmed,
            },
            "confidence": 0.70 if vol_confirmed else 0.50,
        }
    return _no_setup("breakout_consolidacao")


def detect_pullback_tendencia(df_ohlcv: pd.DataFrame,
                               df_ind: pd.DataFrame) -> dict:
    """
    Setup: Pullback em Tendência
    Critério:
    - Tendência de médio prazo = up (sma20 > sma50, preço > sma50)
    - RSI entre 35-55 (pullback acontecendo)
    - Preço próximo (±3%) da SMA20
    - ATR não explodindo
    """
    if df_ind.empty or len(df_ind) < 50:
        return _no_setup("pullback_tendencia")

    last = df_ind.iloc[-1]
    close = df_ohlcv.iloc[-1]["close"]

    sma20  = last.get("sma20")
    sma50  = last.get("sma50")
    rsi    = last.get("rsi14")
    trend  = last.get("trend_mid")

    if any(pd.isna(v) or v is None for v in [sma20, sma50, rsi]):
        return _no_setup("pullback_tendencia")

    trend_up      = (trend == "up") or (float(sma20) > float(sma50) and float(close) > float(sma50))
    rsi_pullback  = 35 <= float(rsi) <= 55
    near_sma20    = abs(float(close) - float(sma20)) / float(sma20) <= 0.03

    if trend_up and rsi_pullback and near_sma20:
        stop   = float(sma50) * 0.98
        risk   = float(close) - stop
        if risk <= 0:
            return _no_setup("pullback_tendencia")
        target = float(close) + risk * 2.0

        # Look for recent highs as target
        recent_high = df_ohlcv.tail(40)["high"].max()
        if recent_high > float(close):
            target = max(target, recent_high)

        return {
            "detected": True,
            "setup":    "pullback_tendencia",
            "name":     SETUP_DEFINITIONS["pullback_tendencia"]["name"],
            "direction":"long",
            "entry":    float(close),
            "stop":     round(stop, 2),
            "target":   round(target, 2),
            "rsi":      round(float(rsi), 1),
            "criteria": {
                "trend_up":     trend_up,
                "rsi_pullback": rsi_pullback,
                "near_sma20":   near_sma20,
            },
            "confidence": 0.65,
        }
    return _no_setup("pullback_tendencia")


def detect_continuacao_momentum(df_ohlcv: pd.DataFrame,
                                  df_ind: pd.DataFrame) -> dict:
    """
    Setup: Continuação de Momentum
    Critério:
    - RSI > 55 (forte)
    - Preço acima de SMA20, SMA50 e SMA200 (tendência longa)
    - Momentum 20d > 5%
    - Força relativa positiva
    """
    if df_ind.empty or len(df_ind) < 200:
        return _no_setup("continuacao_momentum")

    last  = df_ind.iloc[-1]
    close = df_ohlcv.iloc[-1]["close"]

    rsi    = last.get("rsi14")
    sma20  = last.get("sma20")
    sma50  = last.get("sma50")
    sma200 = last.get("sma200")
    mom    = last.get("momentum")
    rs     = last.get("rel_strength")

    if any(pd.isna(v) or v is None for v in [rsi, sma20, sma50, sma200]):
        return _no_setup("continuacao_momentum")

    above_all  = float(close) > float(sma20) > float(sma50) > float(sma200)
    strong_rsi = float(rsi) > config.MOMENTUM_RSI_MIN
    strong_mom = mom is not None and float(mom) > 0.05
    rs_pos     = rs is not None and float(rs) > 0

    score = sum([above_all, strong_rsi, strong_mom, rs_pos])

    if score >= 3:
        stop   = float(sma20) * 0.97
        risk   = float(close) - stop
        if risk <= 0:
            return _no_setup("continuacao_momentum")
        target = float(close) + risk * 2.0

        return {
            "detected":  True,
            "setup":     "continuacao_momentum",
            "name":      SETUP_DEFINITIONS["continuacao_momentum"]["name"],
            "direction": "long",
            "entry":     float(close),
            "stop":      round(stop, 2),
            "target":    round(target, 2),
            "rsi":       round(float(rsi), 1),
            "momentum":  round(float(mom or 0), 4),
            "criteria": {
                "above_all_mas": above_all,
                "strong_rsi":    strong_rsi,
                "strong_mom":    strong_mom,
                "rs_positive":   rs_pos,
            },
            "confidence": 0.55 + (score - 3) * 0.1,
        }
    return _no_setup("continuacao_momentum")


def detect_reversao_confirmada(df_ohlcv: pd.DataFrame,
                                 df_ind: pd.DataFrame) -> dict:
    """
    Setup: Reversão com Confirmação
    Critério:
    - RSI saindo de sobrevenda (< 30 → cruzando acima de 35)
    - MACD hist virando positivo
    - Preço fechando acima da mínima recente
    """
    if df_ind.empty or len(df_ind) < 30:
        return _no_setup("reversao_confirmada")

    last  = df_ind.iloc[-1]
    prev  = df_ind.iloc[-2] if len(df_ind) > 1 else last
    close = df_ohlcv.iloc[-1]["close"]

    rsi_now  = last.get("rsi14")
    rsi_prev = prev.get("rsi14")
    macd_h   = last.get("macd_hist")
    macd_h_p = prev.get("macd_hist")

    if any(pd.isna(v) or v is None for v in [rsi_now, rsi_prev]):
        return _no_setup("reversao_confirmada")

    rsi_from_oversold = (float(rsi_prev) < 30 and float(rsi_now) >= 30)
    macd_turning      = (macd_h is not None and macd_h_p is not None and
                         float(macd_h_p) < 0 and float(macd_h) > float(macd_h_p))

    recent_low = df_ohlcv.tail(10)["low"].min()
    price_above_low = float(close) > float(recent_low)

    if rsi_from_oversold and macd_turning and price_above_low:
        stop   = float(recent_low) * 0.99
        risk   = float(close) - stop
        if risk <= 0:
            return _no_setup("reversao_confirmada")
        target = float(close) + risk * 1.8

        return {
            "detected":  True,
            "setup":     "reversao_confirmada",
            "name":      SETUP_DEFINITIONS["reversao_confirmada"]["name"],
            "direction": "long",
            "entry":     float(close),
            "stop":      round(stop, 2),
            "target":    round(target, 2),
            "rsi":       round(float(rsi_now), 1),
            "criteria": {
                "rsi_from_oversold": rsi_from_oversold,
                "macd_turning":      macd_turning,
                "price_above_low":   price_above_low,
            },
            "confidence": 0.55,
        }
    return _no_setup("reversao_confirmada")


def detect_inside_bar_breakout(df_ohlcv: pd.DataFrame,
                                 df_ind: pd.DataFrame) -> dict:
    """
    Setup: Inside Bar Rompimento
    Critério:
    - Último candle é inside bar (range dentro do anterior)
    - Tendência de médio prazo definida
    """
    if len(df_ohlcv) < 5 or df_ind.empty:
        return _no_setup("inside_bar_breakout")

    prev = df_ohlcv.iloc[-2]
    last = df_ohlcv.iloc[-1]
    last_ind = df_ind.iloc[-1]

    is_inside = (float(last["high"]) < float(prev["high"]) and
                 float(last["low"])  > float(prev["low"]))

    trend = last_ind.get("trend_mid")

    if is_inside and trend == "up":
        entry   = float(prev["high"]) * 1.001  # break above mother bar
        stop    = float(last["low"]) * 0.999
        risk    = entry - stop
        if risk <= 0:
            return _no_setup("inside_bar_breakout")
        target  = entry + risk * 2.0

        return {
            "detected":  True,
            "setup":     "inside_bar_breakout",
            "name":      SETUP_DEFINITIONS["inside_bar_breakout"]["name"],
            "direction": "long",
            "entry":     round(entry, 2),
            "stop":      round(stop, 2),
            "target":    round(target, 2),
            "criteria": {
                "is_inside_bar": is_inside,
                "trend_up":      trend == "up",
            },
            "confidence": 0.60,
        }
    return _no_setup("inside_bar_breakout")


# ─── Master Detector ──────────────────────────────────────────────────────────

def detect_all_setups(ticker: str) -> list[dict]:
    """
    Run all setup detectors for a ticker.
    Returns list of detected setups (may be empty).
    """
    df_ohlcv = get_ohlcv_df(ticker, days=300)
    df_ind   = get_indicators_df(ticker, days=300)

    if df_ohlcv.empty or df_ind.empty:
        return []

    # Merge indicators into ohlcv by date
    if "date" in df_ohlcv.columns and "date" in df_ind.columns:
        df_ohlcv["date"] = pd.to_datetime(df_ohlcv["date"])
        df_ind["date"]   = pd.to_datetime(df_ind["date"])
        df_ind_m = df_ind.copy()
    else:
        df_ind_m = df_ind.copy()

    detectors = [
        detect_breakout_consolidacao,
        detect_pullback_tendencia,
        detect_continuacao_momentum,
        detect_reversao_confirmada,
        detect_inside_bar_breakout,
    ]

    found = []
    for detector in detectors:
        try:
            result = detector(df_ohlcv, df_ind_m)
            if result.get("detected"):
                result["ticker"] = ticker
                found.append(result)
        except Exception as e:
            logger.debug(f"Setup detector error {ticker}: {e}")

    return found


def get_setup_stats(setup_name: str, regime: str = None) -> dict:
    """Return historical performance stats for a setup."""
    conn = get_connection()
    try:
        if regime:
            row = conn.execute(
                """SELECT * FROM setup_stats WHERE setup_name=? AND regime=?""",
                (setup_name, regime)
            ).fetchone()
        else:
            row = conn.execute(
                """SELECT * FROM setup_stats WHERE setup_name=?
                   ORDER BY total_trades DESC LIMIT 1""",
                (setup_name,)
            ).fetchone()

        if row:
            return dict(row)

        # Defaults if no historical data yet
        return _default_setup_stats(setup_name)
    finally:
        conn.close()


def _default_setup_stats(setup_name: str) -> dict:
    """Conservative default stats when no historical data exists."""
    defaults = {
        "breakout_consolidacao": {"win_rate": 0.52, "avg_gain_pct": 6.0,  "avg_loss_pct": -3.0, "avg_duration": 15},
        "pullback_tendencia":    {"win_rate": 0.55, "avg_gain_pct": 5.5,  "avg_loss_pct": -2.5, "avg_duration": 12},
        "continuacao_momentum":  {"win_rate": 0.50, "avg_gain_pct": 8.0,  "avg_loss_pct": -4.0, "avg_duration": 25},
        "reversao_confirmada":   {"win_rate": 0.48, "avg_gain_pct": 5.0,  "avg_loss_pct": -3.5, "avg_duration": 10},
        "inside_bar_breakout":   {"win_rate": 0.53, "avg_gain_pct": 4.5,  "avg_loss_pct": -2.5, "avg_duration": 8},
    }
    base = defaults.get(setup_name, {"win_rate": 0.50, "avg_gain_pct": 5.0,
                                      "avg_loss_pct": -3.0, "avg_duration": 15})
    base.update({
        "setup_name":    setup_name,
        "total_trades":  0,
        "payoff":        abs(base["avg_gain_pct"] / base["avg_loss_pct"]),
        "expected_value": base["win_rate"] * base["avg_gain_pct"] +
                          (1 - base["win_rate"]) * base["avg_loss_pct"],
    })
    return base


# ─── Helper ───────────────────────────────────────────────────────────────────

def _no_setup(name: str) -> dict:
    return {"detected": False, "setup": name}
