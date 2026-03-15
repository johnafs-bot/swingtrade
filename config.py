import os
from pathlib import Path

BASE_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
LOGS_DIR = BASE_DIR / "logs"
DB_PATH = DATA_DIR / "swing_system.db"

# Ensure dirs exist
DATA_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)

# ─── System Defaults ────────────────────────────────────────────────────────
MAX_HORIZON_DAYS       = 90
RISK_PER_TRADE_PCT     = 1.0     # % of portfolio
MAX_POSITIONS          = 10
UPDATE_FREQUENCY       = "daily"
DEFAULT_STRATEGY       = "mixed"  # technical | fundamental | mixed
MIN_VOLUME_DAILY_BRL   = 5_000_000   # R$ 5 milhões médio diário
MIN_PRICE              = 3.0         # R$ mínimo por ação
MIN_HISTORY_DAYS       = 252         # 1 ano de histórico mínimo

# ─── Technical Parameters ────────────────────────────────────────────────────
SMA_SHORT   = 20
SMA_MID     = 50
SMA_LONG    = 200
EMA_FAST    = 9
EMA_SLOW    = 21
ATR_PERIOD  = 14
RSI_PERIOD  = 14
BB_PERIOD   = 20
BB_STD      = 2.0
VOL_PERIOD  = 20   # histórica anualizada
RVOL_PERIOD = 20   # volume relativo

# ─── Setup Thresholds ────────────────────────────────────────────────────────
BREAKOUT_VOL_MULT      = 1.5   # volume mínimo no rompimento
CONSOLIDATION_ATR_MULT = 0.5   # amplitude ATR durante consolidação
PULLBACK_RSI_MAX       = 50    # RSI máximo num pullback em tendência de alta
MOMENTUM_RSI_MIN       = 55    # RSI mínimo para momentum positivo

# ─── Risk/Return ──────────────────────────────────────────────────────────────
MIN_RISK_REWARD        = 2.0   # R:R mínimo para recomendar entrada
MIN_EXP_VALUE          = 0.2   # expectativa matemática mínima (em R)
MIN_WIN_RATE           = 0.45  # probabilidade mínima de sucesso

# ─── Fundamental Thresholds ──────────────────────────────────────────────────
MAX_NET_DEBT_EBITDA    = 3.5
MIN_ROE                = 0.05  # 5%
MIN_EBITDA_MARGIN      = 0.08  # 8%
MIN_REVENUE_GROWTH_YOY = -0.10 # aceita queda de até 10%
MAX_PL_RATIO           = 60.0
MAX_EV_EBIT            = 25.0

# ─── Portfolio Limits ────────────────────────────────────────────────────────
MAX_SECTOR_EXPOSURE_PCT  = 30.0  # máximo por setor
MAX_SINGLE_ASSET_PCT     = 15.0  # máximo por ativo
MAX_PORTFOLIO_RISK_PCT   = 8.0   # risco total máximo da carteira

# ─── Market Regime Thresholds ────────────────────────────────────────────────
REGIME_IBOV_SMA_FAST  = 20
REGIME_IBOV_SMA_SLOW  = 50
REGIME_VIX_HIGH       = 30.0
REGIME_ADVANCE_DECLINE_BULL = 0.55

# ─── Data Collection ─────────────────────────────────────────────────────────
HISTORY_YEARS = 5
BENCHMARK     = "^BVSP"          # Ibovespa
BENCHMARK_SA  = "BOVA11.SA"      # ETF do Ibovespa

# ─── Decision Labels ─────────────────────────────────────────────────────────
ACTIONS = ["ignorar", "observar", "pre_compra", "comprar_pequeno",
           "comprar_normal", "manter", "reduzir", "sair"]

SECRET_KEY = os.environ.get("SECRET_KEY", "swing-b3-secret-2024")
