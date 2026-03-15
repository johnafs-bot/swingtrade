"""
SwingB3 — First-time setup
Run this ONCE before starting the app to populate the database with
historical data for all tickers.

Usage:
  python setup.py [--full] [--tickers PETR4,VALE3,...]

Options:
  --full      Download full history (5 years) for all tickers
  --tickers   Comma-separated list (default: all B3_UNIVERSE)
  --backtest  Run initial backtest after loading data
"""
import sys
import os
import argparse
import logging

sys.path.insert(0, os.path.dirname(__file__))

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser(description="SwingB3 First-time Setup")
    parser.add_argument("--full",     action="store_true", help="Download full history")
    parser.add_argument("--tickers",  type=str,            help="Comma-separated tickers")
    parser.add_argument("--backtest", action="store_true", help="Run initial backtest")
    args = parser.parse_args()

    # Init DB
    logger.info("Initializing database...")
    from database.connection import init_db
    init_db()

    # Seed assets
    logger.info("Seeding assets...")
    from modules.universe import seed_assets, B3_UNIVERSE
    seed_assets()

    # Determine tickers
    if args.tickers:
        tickers = [t.strip().upper() for t in args.tickers.split(",")]
    else:
        tickers = list(B3_UNIVERSE.keys())

    logger.info(f"Will process {len(tickers)} tickers (full={args.full})")

    # Download OHLCV
    logger.info("Downloading OHLCV data (this may take 5-15 minutes)...")
    from modules.data_collector import bulk_update_ohlcv
    result = bulk_update_ohlcv(tickers, full=args.full, delay=0.4)
    logger.info(f"OHLCV done: {result}")

    # Compute technical indicators
    logger.info("Computing technical indicators...")
    from modules.technical_analysis import compute_and_store_indicators
    for i, ticker in enumerate(tickers):
        try:
            compute_and_store_indicators(ticker)
            if (i + 1) % 10 == 0:
                logger.info(f"  Indicators: {i+1}/{len(tickers)}")
        except Exception as e:
            logger.error(f"  Indicator error {ticker}: {e}")

    # Download fundamentals
    logger.info("Downloading fundamentals (this may take a few minutes)...")
    from modules.data_collector import bulk_update_fundamentals
    bulk_update_fundamentals(tickers, delay=0.3)

    # Evaluate fundamentals
    logger.info("Evaluating fundamentals...")
    from modules.fundamental_filter import bulk_evaluate
    bulk_evaluate(tickers)

    # Compute market regime
    logger.info("Computing market regime...")
    from modules.market_regime import classify_regime
    regime = classify_regime()
    logger.info(f"Current regime: {regime.get('label')}")

    # Initial profile
    logger.info("Creating default user profile...")
    from modules.portfolio import save_user_profile
    save_user_profile({
        "total_capital":        100000,
        "free_capital":         100000,
        "horizon_days":         90,
        "risk_per_trade_pct":   1.0,
        "max_loss_pct":         10.0,
        "max_positions":        10,
        "forbidden_sectors":    "",
        "volatility_tolerance": "medium",
        "strategy":             "mixed",
    })

    # Backtest (optional)
    if args.backtest:
        logger.info("Running initial backtest...")
        from modules.backtest import run_all_backtests
        sample_tickers = tickers[:10]
        results = run_all_backtests(sample_tickers, start="2021-01-01")
        for name, res in results.items():
            logger.info(f"  {name}: WR={res.get('win_rate',0):.0%} EV={res.get('expected_value',0):.2f}%")

    logger.info("=" * 50)
    logger.info("Setup COMPLETE! Start the app with:")
    logger.info("  python run.py")
    logger.info("Then open: http://localhost:5050")
    logger.info("=" * 50)


if __name__ == "__main__":
    main()
