"""
Módulo 3 — Coleta de Dados
Importa cotações históricas OHLCV, fundamentos e eventos corporativos da B3.
Usa yfinance para preços e yahooquery para fundamentos.
"""

import logging
import sqlite3
import time
from datetime import datetime, timedelta
from typing import Optional

import pandas as pd
import yfinance as yf
from curl_cffi import requests as cffi_requests

from database.connection import get_connection
from modules.universe import B3_UNIVERSE, get_all_tickers
import config

logger = logging.getLogger(__name__)

# Sessão curl_cffi que impersona Chrome — contorna bloqueio TLS do Yahoo Finance
_YF_SESSION = cffi_requests.Session(impersonate="chrome110")


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _yf_ticker(ticker: str) -> str:
    """Convert B3 ticker to Yahoo Finance format (add .SA)."""
    t = ticker.strip()
    if not t.endswith(".SA"):
        t = t + ".SA"
    return t


def _last_stored_date(ticker: str) -> Optional[str]:
    """Return the most recent date stored for a ticker."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT MAX(date) AS d FROM ohlcv WHERE ticker=?", (ticker,)
        ).fetchone()
        return row["d"] if row else None
    finally:
        conn.close()


# ─── OHLCV ────────────────────────────────────────────────────────────────────

def fetch_ohlcv(ticker: str, start: str, end: str = None) -> pd.DataFrame:
    """Download OHLCV from Yahoo Finance for a B3 ticker."""
    yf_sym = _yf_ticker(ticker)
    try:
        df = yf.download(
            yf_sym,
            start=start,
            end=end or datetime.today().strftime("%Y-%m-%d"),
            auto_adjust=True,
            progress=False,
            multi_level_index=False,  # yfinance >= 0.2.40 flat columns
            session=_YF_SESSION,
        )
        if df.empty:
            return pd.DataFrame()

        df = df.reset_index()

        # Flatten MultiIndex columns if present (older yfinance versions)
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                "_".join([str(x) for x in col if str(x) not in ("", ticker, yf_sym)]).strip("_").lower()
                or str(col[0]).lower()
                for col in df.columns
            ]
        else:
            # Single-level — convert tuple elements to str safely
            new_cols = []
            for c in df.columns:
                if isinstance(c, tuple):
                    new_cols.append("_".join(str(x) for x in c if x).lower())
                else:
                    new_cols.append(str(c).lower().replace(" ", "_"))
            df.columns = new_cols

        # Normalize column names
        col_map = {}
        for c in df.columns:
            lc = str(c).lower()
            if "date" in lc:                          col_map[c] = "date"
            elif "open" in lc:                        col_map[c] = "open"
            elif "high" in lc:                        col_map[c] = "high"
            elif "low" in lc:                         col_map[c] = "low"
            elif "close" in lc and "adj" not in lc:   col_map[c] = "close"
            elif "adj" in lc:                         col_map[c] = "adj_close"
            elif "volume" in lc:                      col_map[c] = "volume"
        df = df.rename(columns=col_map)
        df["ticker"] = ticker
        df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")

        needed = ["ticker", "date", "open", "high", "low", "close", "volume"]
        for col in needed:
            if col not in df.columns:
                df[col] = None
        df["adj_close"] = df.get("adj_close", df["close"])
        return df[needed + ["adj_close"]].dropna(subset=["close"])

    except Exception as e:
        logger.warning(f"OHLCV error {ticker}: {e}")
        return pd.DataFrame()


def store_ohlcv(df: pd.DataFrame):
    """Store OHLCV records, skip duplicates."""
    if df.empty:
        return 0
    conn = get_connection()
    inserted = 0
    try:
        for _, row in df.iterrows():
            try:
                conn.execute(
                    """INSERT OR IGNORE INTO ohlcv
                       (ticker, date, open, high, low, close, volume, adj_close)
                       VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
                    (row["ticker"], row["date"], row.get("open"), row.get("high"),
                     row.get("low"), row.get("close"), row.get("volume"),
                     row.get("adj_close"))
                )
                inserted += 1
            except Exception:
                pass
        conn.commit()
    finally:
        conn.close()
    return inserted


def update_ohlcv(ticker: str, full: bool = False):
    """
    Update OHLCV for a single ticker.
    full=True fetches full history (HISTORY_YEARS).
    full=False only fetches from last stored date.
    """
    last = _last_stored_date(ticker)
    if full or not last:
        start = (datetime.today() - timedelta(days=config.HISTORY_YEARS * 365)).strftime("%Y-%m-%d")
    else:
        # Start from next day after last stored
        start = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    today = datetime.today().strftime("%Y-%m-%d")
    if start >= today:
        logger.debug(f"{ticker}: already up to date.")
        return 0

    df = fetch_ohlcv(ticker, start)
    if df.empty:
        return 0

    n = store_ohlcv(df)
    logger.debug(f"{ticker}: {n} rows stored.")
    return n


def batch_fetch_ohlcv(tickers: list, start: str, end: str = None) -> pd.DataFrame:
    """
    Baixa OHLCV de múltiplos tickers em uma única chamada yf.download.
    Retorna DataFrame no formato longo: ticker, date, open, high, low, close, volume.
    """
    yf_symbols = [_yf_ticker(t) for t in tickers]
    end = end or datetime.today().strftime("%Y-%m-%d")
    try:
        raw = yf.download(
            yf_symbols,
            start=start,
            end=end,
            auto_adjust=True,
            progress=False,
            group_by="ticker",
            session=_YF_SESSION,
        )
        if raw.empty:
            return pd.DataFrame()

        frames = []
        for sym, ticker in zip(yf_symbols, tickers):
            try:
                if raw.columns.nlevels == 2:
                    if sym not in raw.columns.get_level_values(0):
                        continue
                    sub = raw[sym].copy().reset_index()
                else:
                    # Apenas 1 ticker retornado como flat
                    sub = raw.copy().reset_index()

                sub.columns = [str(c).lower().replace(" ", "_") for c in sub.columns]
                rename = {}
                for c in sub.columns:
                    if "date" in c:      rename[c] = "date"
                    elif c == "open":    rename[c] = "open"
                    elif c == "high":    rename[c] = "high"
                    elif c == "low":     rename[c] = "low"
                    elif c == "close":   rename[c] = "close"
                    elif c == "volume":  rename[c] = "volume"
                sub = sub.rename(columns=rename)
                sub["ticker"] = ticker
                sub["adj_close"] = sub.get("close", pd.Series(dtype=float))
                sub["date"] = pd.to_datetime(sub["date"]).dt.strftime("%Y-%m-%d")
                needed = ["ticker", "date", "open", "high", "low", "close", "volume", "adj_close"]
                for col in needed:
                    if col not in sub.columns:
                        sub[col] = None
                sub = sub[needed].dropna(subset=["close"])
                frames.append(sub)
            except Exception as e:
                logger.warning(f"Unpack error {ticker}: {e}")

        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)

    except Exception as e:
        logger.error(f"batch_fetch_ohlcv error: {e}")
        return pd.DataFrame()


def bulk_update_ohlcv(tickers: list = None, full: bool = False, delay: float = 1.0):
    """Atualiza OHLCV para todos os tickers usando download em lotes de 30."""
    tickers = tickers or list(B3_UNIVERSE.keys())

    # Garantir que BOVA11 está na lista (benchmark obrigatório)
    if "BOVA11" not in tickers:
        tickers = list(tickers) + ["BOVA11"]

    # Calcular data de início por ticker
    today = datetime.today().strftime("%Y-%m-%d")
    start_map = {}
    for t in tickers:
        last = _last_stored_date(t)
        if full or not last:
            start_map[t] = (datetime.today() - timedelta(days=config.HISTORY_YEARS * 365)).strftime("%Y-%m-%d")
        else:
            nxt = (datetime.strptime(last, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
            start_map[t] = nxt

    # Filtrar tickers que já estão atualizados
    pending = [t for t in tickers if start_map[t] < today]
    if not pending:
        logger.info("Todos os tickers já estão atualizados.")
        return {"total_rows": 0, "errors": 0}

    BATCH = 30
    total = 0
    errors = 0

    for i in range(0, len(pending), BATCH):
        batch = pending[i:i + BATCH]
        batch_start = min(start_map[t] for t in batch)
        try:
            df = batch_fetch_ohlcv(batch, start=batch_start)
            if not df.empty:
                # Filtrar cada ticker pelo seu start individual
                rows = []
                for t in batch:
                    sub = df[df["ticker"] == t]
                    sub = sub[sub["date"] >= start_map[t]]
                    if not sub.empty:
                        rows.append(sub)
                if rows:
                    df_filtered = pd.concat(rows, ignore_index=True)
                    n = store_ohlcv(df_filtered)
                    total += n
                    logger.info(f"Batch {i//BATCH + 1}: {n} linhas armazenadas para {len(batch)} tickers")
        except Exception as e:
            logger.error(f"Erro no batch {batch}: {e}")
            errors += len(batch)
        time.sleep(delay)

    logger.info(f"Bulk OHLCV done: {total} linhas, {errors} erros.")
    return {"total_rows": total, "errors": errors}


# ─── Fundamentals ─────────────────────────────────────────────────────────────

def fetch_fundamentals_yf(ticker: str) -> dict:
    """
    Fetch fundamental data using yfinance.
    Returns a dict of fundamental metrics.
    """
    yf_sym = _yf_ticker(ticker)
    try:
        stock = yf.Ticker(yf_sym)
        info = stock.info or {}

        def safe(key, default=None):
            v = info.get(key)
            return v if v is not None else default

        revenue          = safe("totalRevenue")
        net_income       = safe("netIncomeToCommon")
        ebitda           = safe("ebitda")
        total_debt       = safe("totalDebt", 0)
        cash             = safe("totalCash", 0)
        net_debt         = (total_debt or 0) - (cash or 0)
        equity           = safe("bookValue", 0) * safe("sharesOutstanding", 0) if safe("bookValue") else None
        net_margin       = safe("profitMargins")
        ebitda_margin    = (ebitda / revenue) if ebitda and revenue else None
        roe              = safe("returnOnEquity")
        roic             = safe("returnOnAssets")  # approximate
        pl_ratio         = safe("trailingPE")
        ev_ebit          = safe("enterpriseToEbitda")
        ev_ebitda        = safe("enterpriseToEbitda")
        dividend_yield   = safe("dividendYield")
        rev_growth       = safe("revenueGrowth")
        income_growth    = safe("earningsGrowth")
        market_cap       = safe("marketCap")

        net_debt_ebitda  = (net_debt / ebitda) if ebitda and ebitda > 0 else None

        return {
            "revenue":            revenue,
            "net_income":         net_income,
            "ebitda":             ebitda,
            "net_debt":           net_debt,
            "equity":             equity,
            "net_margin":         net_margin,
            "ebitda_margin":      ebitda_margin,
            "roe":                roe,
            "roic":               roic,
            "pl_ratio":           pl_ratio,
            "ev_ebit":            ev_ebit,
            "ev_ebitda":          ev_ebitda,
            "dividend_yield":     dividend_yield,
            "revenue_growth_yoy": rev_growth,
            "income_growth_yoy":  income_growth,
            "net_debt_ebitda":    net_debt_ebitda,
            "market_cap":         market_cap,
        }
    except Exception as e:
        logger.warning(f"Fundamentals error {ticker}: {e}")
        return {}


def store_fundamentals(ticker: str, data: dict):
    """Store fundamental data with today's date."""
    if not data:
        return
    today = datetime.today().strftime("%Y-%m-%d")
    conn = get_connection()
    try:
        conn.execute(
            """INSERT OR REPLACE INTO fundamentals
               (ticker, date, revenue, net_income, ebitda, net_debt, equity,
                net_margin, ebitda_margin, roe, roic, pl_ratio, ev_ebit, ev_ebitda,
                dividend_yield, revenue_growth_yoy, income_growth_yoy,
                net_debt_ebitda, market_cap)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (ticker, today,
             data.get("revenue"),      data.get("net_income"),   data.get("ebitda"),
             data.get("net_debt"),     data.get("equity"),       data.get("net_margin"),
             data.get("ebitda_margin"),data.get("roe"),          data.get("roic"),
             data.get("pl_ratio"),     data.get("ev_ebit"),      data.get("ev_ebitda"),
             data.get("dividend_yield"),data.get("revenue_growth_yoy"),
             data.get("income_growth_yoy"),data.get("net_debt_ebitda"),
             data.get("market_cap"))
        )
        conn.commit()
    finally:
        conn.close()


def bulk_update_fundamentals(tickers: list = None, delay: float = 0.5):
    """Update fundamentals for all tickers."""
    tickers = tickers or list(B3_UNIVERSE.keys())
    ok = 0
    for ticker in tickers:
        try:
            data = fetch_fundamentals_yf(ticker)
            store_fundamentals(ticker, data)
            ok += 1
        except Exception as e:
            logger.error(f"Fundamental update error {ticker}: {e}")
        time.sleep(delay)
    logger.info(f"Fundamentals updated: {ok}/{len(tickers)}")
    return ok


# ─── Corporate Events ─────────────────────────────────────────────────────────

def fetch_corporate_events(ticker: str):
    """Fetch dividends and splits from yfinance."""
    yf_sym = _yf_ticker(ticker)
    try:
        stock = yf.Ticker(yf_sym)
        events = []

        # Dividends
        divs = stock.dividends
        if divs is not None and not divs.empty:
            for dt, val in divs.items():
                events.append({
                    "ticker": ticker,
                    "date": pd.to_datetime(dt).strftime("%Y-%m-%d"),
                    "event_type": "dividend",
                    "value": float(val),
                    "description": f"Dividendo R$ {val:.4f}",
                })

        # Splits
        splits = stock.splits
        if splits is not None and not splits.empty:
            for dt, val in splits.items():
                events.append({
                    "ticker": ticker,
                    "date": pd.to_datetime(dt).strftime("%Y-%m-%d"),
                    "event_type": "split",
                    "value": float(val),
                    "description": f"Split {val:.2f}:1",
                })

        return events
    except Exception as e:
        logger.warning(f"Events error {ticker}: {e}")
        return []


def store_corporate_events(events: list):
    """Store corporate events (deduplicated)."""
    if not events:
        return
    conn = get_connection()
    try:
        for ev in events:
            # Check if already stored
            existing = conn.execute(
                """SELECT id FROM corporate_events
                   WHERE ticker=? AND date=? AND event_type=?""",
                (ev["ticker"], ev["date"], ev["event_type"])
            ).fetchone()
            if not existing:
                conn.execute(
                    """INSERT INTO corporate_events
                       (ticker, date, event_type, value, description)
                       VALUES (?, ?, ?, ?, ?)""",
                    (ev["ticker"], ev["date"], ev["event_type"],
                     ev.get("value"), ev.get("description"))
                )
        conn.commit()
    finally:
        conn.close()


def run_daily_update(tickers: list = None):
    """
    Run complete daily update:
    1. OHLCV incremental
    2. Fundamentals
    3. Corporate events
    """
    from modules.technical_analysis import compute_and_store_indicators
    tickers = tickers or list(B3_UNIVERSE.keys())

    logger.info("=== Daily update started ===")

    # 1. OHLCV
    ohlcv_result = bulk_update_ohlcv(tickers, full=False)
    logger.info(f"OHLCV: {ohlcv_result}")

    # 2. Technical indicators
    logger.info("Computing technical indicators...")
    for ticker in tickers:
        try:
            compute_and_store_indicators(ticker)
        except Exception as e:
            logger.error(f"Indicator error {ticker}: {e}")

    # 3. Fundamentals (weekly refresh is enough — do if no data today)
    today = datetime.today().strftime("%Y-%m-%d")
    conn = get_connection()
    fund_pending = []
    for ticker in tickers:
        row = conn.execute(
            "SELECT date FROM fundamentals WHERE ticker=? ORDER BY date DESC LIMIT 1",
            (ticker,)
        ).fetchone()
        if not row or row["date"] < today:
            fund_pending.append(ticker)
    conn.close()

    if fund_pending:
        logger.info(f"Updating fundamentals for {len(fund_pending)} tickers...")
        bulk_update_fundamentals(fund_pending)

    # 4. Corporate events
    logger.info("Updating corporate events...")
    for ticker in tickers[:20]:  # limit to avoid rate limits
        try:
            events = fetch_corporate_events(ticker)
            store_corporate_events(events)
        except Exception as e:
            logger.error(f"Events error {ticker}: {e}")

    logger.info("=== Daily update complete ===")
    return {"status": "ok", "tickers": len(tickers), "ohlcv": ohlcv_result}


def get_ohlcv_df(ticker: str, days: int = 500) -> pd.DataFrame:
    """Load OHLCV from database as DataFrame."""
    conn = get_connection()
    try:
        query = """
        SELECT date, open, high, low, close, volume, adj_close
        FROM ohlcv
        WHERE ticker=?
        ORDER BY date DESC
        LIMIT ?
        """
        df = pd.read_sql_query(query, conn, params=(ticker, days))
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").reset_index(drop=True)
        return df
    finally:
        conn.close()
