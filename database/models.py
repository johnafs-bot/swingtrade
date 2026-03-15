"""
SQLite schema definitions.
All CREATE TABLE statements as a list.
"""

CREATE_STATEMENTS = [

    # ── Assets ────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS assets (
        ticker          TEXT PRIMARY KEY,
        name            TEXT,
        sector          TEXT,
        subsector       TEXT,
        asset_type      TEXT DEFAULT 'ON',  -- ON, PN, UNIT, FII, ETF
        is_active       INTEGER DEFAULT 1,
        created_at      TEXT DEFAULT (datetime('now'))
    )
    """,

    # ── OHLCV ─────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS ohlcv (
        id        INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker    TEXT NOT NULL,
        date      TEXT NOT NULL,
        open      REAL,
        high      REAL,
        low       REAL,
        close     REAL,
        volume    REAL,
        adj_close REAL,
        UNIQUE(ticker, date),
        FOREIGN KEY (ticker) REFERENCES assets(ticker)
    )
    """,

    # ── Technical Indicators ──────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS technical_indicators (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT NOT NULL,
        date            TEXT NOT NULL,
        sma20           REAL,
        sma50           REAL,
        sma200          REAL,
        ema9            REAL,
        ema21           REAL,
        rsi14           REAL,
        atr14           REAL,
        bb_upper        REAL,
        bb_middle       REAL,
        bb_lower        REAL,
        macd            REAL,
        macd_signal     REAL,
        macd_hist       REAL,
        vol_hist        REAL,   -- volatilidade historica anualizada
        rvol            REAL,   -- volume relativo
        adx             REAL,
        trend_short     TEXT,   -- up/down/lateral
        trend_mid       TEXT,
        trend_long      TEXT,
        momentum        REAL,   -- % retorno 20 dias
        rel_strength    REAL,   -- retorno ativo vs IBOV 63d
        dist_sma20      REAL,   -- distancia % do preco a SMA20
        dist_sma50      REAL,
        dist_sma200     REAL,
        UNIQUE(ticker, date),
        FOREIGN KEY (ticker) REFERENCES assets(ticker)
    )
    """,

    # ── Fundamentals ──────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS fundamentals (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker              TEXT NOT NULL,
        date                TEXT NOT NULL,
        revenue             REAL,
        net_income          REAL,
        ebitda              REAL,
        net_debt            REAL,
        equity              REAL,
        net_margin          REAL,
        ebitda_margin       REAL,
        roe                 REAL,
        roic                REAL,
        pl_ratio            REAL,
        ev_ebit             REAL,
        ev_ebitda           REAL,
        dividend_yield      REAL,
        revenue_growth_yoy  REAL,
        income_growth_yoy   REAL,
        net_debt_ebitda     REAL,
        market_cap          REAL,
        fundamental_score   REAL,
        fundamental_grade   TEXT,  -- A/B/C/D/F
        UNIQUE(ticker, date),
        FOREIGN KEY (ticker) REFERENCES assets(ticker)
    )
    """,

    # ── Corporate Events ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS corporate_events (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker      TEXT NOT NULL,
        date        TEXT NOT NULL,
        event_type  TEXT,   -- dividend, split, bonus, follow_on, relevant_fact
        value       REAL,
        description TEXT,
        created_at  TEXT DEFAULT (datetime('now')),
        FOREIGN KEY (ticker) REFERENCES assets(ticker)
    )
    """,

    # ── User Profile ──────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS user_profile (
        id                      INTEGER PRIMARY KEY AUTOINCREMENT,
        total_capital           REAL DEFAULT 100000,
        free_capital            REAL DEFAULT 100000,
        horizon_days            INTEGER DEFAULT 90,
        risk_per_trade_pct      REAL DEFAULT 1.0,
        max_loss_pct            REAL DEFAULT 10.0,
        max_positions           INTEGER DEFAULT 10,
        forbidden_sectors       TEXT DEFAULT '',
        volatility_tolerance    TEXT DEFAULT 'medium',
        preferred_setups        TEXT DEFAULT 'all',
        strategy                TEXT DEFAULT 'mixed',
        updated_at              TEXT DEFAULT (datetime('now'))
    )
    """,

    # ── Portfolio Positions ───────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS portfolio (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker          TEXT NOT NULL,
        quantity        REAL NOT NULL,
        avg_price       REAL NOT NULL,
        current_price   REAL,
        date_opened     TEXT,
        stop_price      REAL,
        target_price    REAL,
        setup_name      TEXT,
        status          TEXT DEFAULT 'open',  -- open/closed
        closed_at       TEXT,
        pnl             REAL,
        pnl_pct         REAL,
        notes           TEXT,
        FOREIGN KEY (ticker) REFERENCES assets(ticker)
    )
    """,

    # ── Decisions History ─────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS decisions (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker              TEXT NOT NULL,
        date                TEXT NOT NULL,
        action              TEXT NOT NULL,
        probability         REAL,
        risk_pct            REAL,
        expected_return_pct REAL,
        expected_value      REAL,
        suggested_size_pct  REAL,
        stop_price          REAL,
        target_price        REAL,
        setup_name          TEXT,
        regime              TEXT,
        fundamental_grade   TEXT,
        justification       TEXT,
        created_at          TEXT DEFAULT (datetime('now'))
    )
    """,

    # ── Alerts ────────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS alerts (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        ticker      TEXT,
        alert_type  TEXT,   -- new_opportunity, stop_hit, regime_change, etc.
        message     TEXT,
        priority    TEXT DEFAULT 'medium',  -- low/medium/high/critical
        is_read     INTEGER DEFAULT 0,
        created_at  TEXT DEFAULT (datetime('now'))
    )
    """,

    # ── Backtest Results ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS backtest_results (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        setup_name      TEXT NOT NULL,
        ticker_filter   TEXT,
        date_start      TEXT,
        date_end        TEXT,
        total_trades    INTEGER,
        win_rate        REAL,
        avg_gain        REAL,
        avg_loss        REAL,
        payoff          REAL,
        expected_value  REAL,
        max_drawdown    REAL,
        total_return    REAL,
        sharpe_ratio    REAL,
        params          TEXT,    -- JSON com parâmetros usados
        created_at      TEXT DEFAULT (datetime('now'))
    )
    """,

    # ── Setup Statistics ──────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS setup_stats (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        setup_name      TEXT NOT NULL,
        regime          TEXT,
        total_trades    INTEGER DEFAULT 0,
        wins            INTEGER DEFAULT 0,
        losses          INTEGER DEFAULT 0,
        win_rate        REAL DEFAULT 0,
        avg_gain_pct    REAL DEFAULT 0,
        avg_loss_pct    REAL DEFAULT 0,
        payoff          REAL DEFAULT 0,
        expected_value  REAL DEFAULT 0,
        avg_duration    REAL DEFAULT 0,
        updated_at      TEXT DEFAULT (datetime('now')),
        UNIQUE(setup_name, regime)
    )
    """,

    # ── Market Regime History ─────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS market_regime (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        date            TEXT NOT NULL UNIQUE,
        regime          TEXT,   -- bull/bear/lateral/high_vol/low_vol
        ibov_trend      TEXT,
        ibov_sma20      REAL,
        ibov_sma50      REAL,
        ibov_close      REAL,
        advance_decline REAL,
        avg_volatility  REAL,
        description     TEXT,
        created_at      TEXT DEFAULT (datetime('now'))
    )
    """,

    # ── Ranking ───────────────────────────────────────────────────────────────
    """
    CREATE TABLE IF NOT EXISTS daily_ranking (
        id                  INTEGER PRIMARY KEY AUTOINCREMENT,
        date                TEXT NOT NULL,
        ticker              TEXT NOT NULL,
        rank_position       INTEGER,
        signal_score        REAL,
        fundamental_score   REAL,
        technical_score     REAL,
        expected_value      REAL,
        probability         REAL,
        risk_reward         REAL,
        regime_score        REAL,
        composite_score     REAL,
        action              TEXT,
        UNIQUE(date, ticker)
    )
    """,

    # Indexes for performance
    "CREATE INDEX IF NOT EXISTS idx_ohlcv_ticker_date ON ohlcv(ticker, date)",
    "CREATE INDEX IF NOT EXISTS idx_tech_ticker_date ON technical_indicators(ticker, date)",
    "CREATE INDEX IF NOT EXISTS idx_fund_ticker_date ON fundamentals(ticker, date)",
    "CREATE INDEX IF NOT EXISTS idx_decisions_date ON decisions(date)",
    "CREATE INDEX IF NOT EXISTS idx_alerts_read ON alerts(is_read)",
]
