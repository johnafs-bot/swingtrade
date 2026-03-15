import sqlite3
import logging
from pathlib import Path
from config import DB_PATH

logger = logging.getLogger(__name__)


def get_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    return conn


def init_db():
    """Create all tables if they don't exist."""
    from database.models import CREATE_STATEMENTS
    conn = get_connection()
    try:
        cur = conn.cursor()
        for stmt in CREATE_STATEMENTS:
            cur.execute(stmt)
        conn.commit()
        logger.info("Database initialized.")
    except Exception as e:
        logger.error(f"DB init error: {e}")
        raise
    finally:
        conn.close()
