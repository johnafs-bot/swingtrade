"""
Módulo 6 — Análise Técnica
Calcula e armazena indicadores técnicos derivados das séries OHLCV.
Toda análise é feita sobre séries numéricas — sem imagem de gráfico.
"""

import logging
import numpy as np
import pandas as pd
from ta import momentum, trend, volatility, volume

from database.connection import get_connection
from modules.data_collector import get_ohlcv_df
import config

logger = logging.getLogger(__name__)


# ─── Indicator Computation ────────────────────────────────────────────────────

def compute_indicators(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute all technical indicators on a OHLCV DataFrame.
    Input columns: date, open, high, low, close, volume
    Returns df with all indicator columns added.
    """
    if df.empty or len(df) < 30:
        return pd.DataFrame()

    c = df["close"]
    h = df["high"]
    l = df["low"]
    v = df["volume"]

    # ── Moving Averages ────────────────────────────────────────────────────────
    df["sma20"]  = trend.sma_indicator(c, window=config.SMA_SHORT,  fillna=False)
    df["sma50"]  = trend.sma_indicator(c, window=config.SMA_MID,    fillna=False)
    df["sma200"] = trend.sma_indicator(c, window=config.SMA_LONG,   fillna=False)
    df["ema9"]   = trend.ema_indicator(c, window=config.EMA_FAST,   fillna=False)
    df["ema21"]  = trend.ema_indicator(c, window=config.EMA_SLOW,   fillna=False)

    # ── Momentum ───────────────────────────────────────────────────────────────
    df["rsi14"]  = momentum.rsi(c, window=config.RSI_PERIOD, fillna=False)

    macd_obj     = trend.MACD(c, window_slow=26, window_fast=12, window_sign=9)
    df["macd"]        = macd_obj.macd()
    df["macd_signal"] = macd_obj.macd_signal()
    df["macd_hist"]   = macd_obj.macd_diff()

    # ── Volatility ────────────────────────────────────────────────────────────
    atr_obj      = volatility.AverageTrueRange(h, l, c, window=config.ATR_PERIOD, fillna=False)
    df["atr14"]  = atr_obj.average_true_range()

    bb_obj       = volatility.BollingerBands(c, window=config.BB_PERIOD, window_dev=config.BB_STD, fillna=False)
    df["bb_upper"]  = bb_obj.bollinger_hband()
    df["bb_middle"] = bb_obj.bollinger_mavg()
    df["bb_lower"]  = bb_obj.bollinger_lband()

    # Historical volatility (annualized std of log returns)
    log_ret      = np.log(c / c.shift(1))
    df["vol_hist"] = log_ret.rolling(config.VOL_PERIOD).std() * np.sqrt(252)

    # ── Volume ────────────────────────────────────────────────────────────────
    avg_vol      = v.rolling(config.RVOL_PERIOD).mean()
    df["rvol"]   = v / avg_vol  # relative volume

    # ── ADX ───────────────────────────────────────────────────────────────────
    adx_obj      = trend.ADXIndicator(h, l, c, window=14, fillna=False)
    df["adx"]    = adx_obj.adx()

    # ── Momentum (20-day return) ──────────────────────────────────────────────
    df["momentum"] = c.pct_change(20)

    # ── Trend Classification ──────────────────────────────────────────────────
    def classify_trend(price, fast, slow, long_ma=None):
        """Returns 'up', 'down', or 'lateral' based on MAs."""
        results = []
        for i in range(len(price)):
            p = price.iloc[i]
            f = fast.iloc[i]
            s = slow.iloc[i]
            l = long_ma.iloc[i] if long_ma is not None else None
            if pd.isna(f) or pd.isna(s):
                results.append(None)
                continue
            if p > f > s and (l is None or p > l):
                results.append("up")
            elif p < f < s and (l is None or p < l):
                results.append("down")
            else:
                results.append("lateral")
        return results

    df["trend_short"] = classify_trend(c, df["ema9"],  df["ema21"])
    df["trend_mid"]   = classify_trend(c, df["sma20"], df["sma50"])
    df["trend_long"]  = classify_trend(c, df["sma50"], df["sma200"])

    # ── Distance to MAs (%) ──────────────────────────────────────────────────
    df["dist_sma20"]  = (c - df["sma20"])  / df["sma20"]
    df["dist_sma50"]  = (c - df["sma50"])  / df["sma50"]
    df["dist_sma200"] = (c - df["sma200"]) / df["sma200"]

    # Relative strength will be filled in separately (vs IBOV)
    df["rel_strength"] = np.nan

    return df


def compute_relative_strength(ticker_df: pd.DataFrame, bench_df: pd.DataFrame,
                               period: int = 63) -> pd.Series:
    """
    Compute relative strength of ticker vs benchmark over `period` days.
    Returns a Series aligned to ticker_df index.
    """
    if ticker_df.empty or bench_df.empty:
        return pd.Series(dtype=float)

    t = ticker_df.set_index("date")["close"]
    b = bench_df.set_index("date")["close"]

    t_ret = t.pct_change(period)
    b_ret = b.pct_change(period)

    # Align
    combined = pd.concat([t_ret, b_ret], axis=1).dropna()
    combined.columns = ["ticker", "bench"]
    rs = combined["ticker"] - combined["bench"]

    # Re-align to original index
    result = ticker_df["date"].map(lambda d: rs.get(d, np.nan))
    return result


def compute_and_store_indicators(ticker: str, days: int = 600):
    """Compute indicators and store results in DB."""
    df = get_ohlcv_df(ticker, days=days)
    if df.empty or len(df) < 60:
        logger.debug(f"Not enough data for {ticker}")
        return

    # Load benchmark for relative strength
    try:
        bench_df = get_ohlcv_df("BOVA11", days=days)
        rs = compute_relative_strength(df, bench_df, period=63)
    except Exception:
        rs = pd.Series([np.nan] * len(df))

    df = compute_indicators(df)
    if df.empty:
        return

    df["rel_strength"] = rs.values if len(rs) == len(df) else np.nan

    conn = get_connection()
    try:
        # Only store the latest 252 rows (1 year) to keep DB lean
        recent = df.tail(252)
        for _, row in recent.iterrows():
            date_str = row["date"].strftime("%Y-%m-%d") if hasattr(row["date"], "strftime") else str(row["date"])
            conn.execute(
                """INSERT OR REPLACE INTO technical_indicators
                   (ticker, date, sma20, sma50, sma200, ema9, ema21, rsi14, atr14,
                    bb_upper, bb_middle, bb_lower, macd, macd_signal, macd_hist,
                    vol_hist, rvol, adx, trend_short, trend_mid, trend_long,
                    momentum, rel_strength, dist_sma20, dist_sma50, dist_sma200)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (
                    ticker, date_str,
                    _n(row.get("sma20")),    _n(row.get("sma50")),
                    _n(row.get("sma200")),   _n(row.get("ema9")),
                    _n(row.get("ema21")),    _n(row.get("rsi14")),
                    _n(row.get("atr14")),    _n(row.get("bb_upper")),
                    _n(row.get("bb_middle")),_n(row.get("bb_lower")),
                    _n(row.get("macd")),     _n(row.get("macd_signal")),
                    _n(row.get("macd_hist")),_n(row.get("vol_hist")),
                    _n(row.get("rvol")),     _n(row.get("adx")),
                    row.get("trend_short"),  row.get("trend_mid"),
                    row.get("trend_long"),
                    _n(row.get("momentum")), _n(row.get("rel_strength")),
                    _n(row.get("dist_sma20")),_n(row.get("dist_sma50")),
                    _n(row.get("dist_sma200")),
                )
            )
        conn.commit()
        logger.debug(f"Indicators stored for {ticker}: {len(recent)} rows")
    except Exception as e:
        logger.error(f"Store indicators error {ticker}: {e}")
    finally:
        conn.close()


def _n(v):
    """Return None if NaN, else float."""
    if v is None:
        return None
    try:
        if np.isnan(float(v)):
            return None
        return float(v)
    except Exception:
        return None


def get_latest_indicators(ticker: str) -> dict:
    """Return the most recent technical indicators for a ticker as dict."""
    conn = get_connection()
    try:
        row = conn.execute(
            """SELECT * FROM technical_indicators
               WHERE ticker=? ORDER BY date DESC LIMIT 1""",
            (ticker,)
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()


def get_indicators_df(ticker: str, days: int = 252) -> pd.DataFrame:
    """Return technical indicators as DataFrame."""
    conn = get_connection()
    try:
        df = pd.read_sql_query(
            """SELECT * FROM technical_indicators
               WHERE ticker=? ORDER BY date DESC LIMIT ?""",
            conn, params=(ticker, days)
        )
        df["date"] = pd.to_datetime(df["date"])
        return df.sort_values("date").reset_index(drop=True)
    finally:
        conn.close()


# ─── Scanner Helpers ──────────────────────────────────────────────────────────

def find_consolidation(df: pd.DataFrame, lookback: int = 15) -> bool:
    """
    Detect consolidation: ATR-based low amplitude over last `lookback` bars.
    """
    if len(df) < lookback + config.ATR_PERIOD:
        return False
    recent = df.tail(lookback)
    atr = recent["atr14"].iloc[-1]
    if pd.isna(atr) or atr == 0:
        return False
    price_range = (recent["high"].max() - recent["low"].min())
    return price_range < (atr * config.CONSOLIDATION_ATR_MULT * lookback)


def find_breakout(df: pd.DataFrame, lookback: int = 20) -> dict:
    """
    Detect breakout above recent high with volume confirmation.
    Returns dict with 'detected', 'direction', 'level', 'volume_confirmed'.
    """
    if len(df) < lookback + 1:
        return {"detected": False}

    recent = df.tail(lookback + 1)
    prev   = recent.iloc[:-1]
    last   = recent.iloc[-1]

    resistance = prev["high"].max()
    support    = prev["low"].min()
    rvol       = last.get("rvol", 1.0)
    if pd.isna(rvol):
        rvol = 1.0

    vol_confirmed = float(rvol) >= config.BREAKOUT_VOL_MULT

    if last["close"] > resistance:
        return {
            "detected": True,
            "direction": "up",
            "level": float(resistance),
            "volume_confirmed": vol_confirmed,
            "rvol": float(rvol),
        }
    if last["close"] < support:
        return {
            "detected": True,
            "direction": "down",
            "level": float(support),
            "volume_confirmed": vol_confirmed,
            "rvol": float(rvol),
        }
    return {"detected": False}


def find_pullback_in_trend(df: pd.DataFrame) -> dict:
    """
    Detect a pullback to MA in an established uptrend.
    """
    if df.empty or len(df) < 60:
        return {"detected": False}

    last = df.iloc[-1]
    close = last["close"]
    sma20 = last.get("sma20")
    sma50 = last.get("sma50")
    rsi   = last.get("rsi14")
    trend = last.get("trend_mid")

    if any(pd.isna(v) for v in [close, sma20, sma50, rsi]):
        return {"detected": False}

    # Uptrend + RSI pulled back to neutral zone
    if (trend == "up" and
            float(rsi) < config.PULLBACK_RSI_MAX and
            float(close) >= float(sma20) * 0.98):
        return {
            "detected": True,
            "type": "pullback_trend_up",
            "rsi": float(rsi),
            "support_level": float(sma20),
        }
    return {"detected": False}
