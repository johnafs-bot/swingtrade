"""
Módulo 17 — Alertas
Gera e armazena alertas automáticos sobre carteira e mercado.
"""

import logging
from datetime import datetime

from database.connection import get_connection
from modules.portfolio import get_portfolio, get_user_profile
import config

logger = logging.getLogger(__name__)

ALERT_TYPES = {
    "new_opportunity":   ("Nova Oportunidade", "high"),
    "stop_hit":          ("Stop Rompido", "critical"),
    "setup_lost":        ("Setup Inválido", "medium"),
    "regime_change":     ("Mudança de Regime", "high"),
    "reduce_position":   ("Reduzir Posição", "medium"),
    "exit_position":     ("Sair da Posição", "critical"),
    "concentration":     ("Concentração Excessiva", "high"),
    "fundamental_change":("Mudança Fundamentalista", "medium"),
    "target_reached":    ("Alvo Atingido", "high"),
    "daily_update":      ("Atualização Diária", "low"),
}


def create_alert(ticker: str, alert_type: str, message: str, priority: str = None):
    """Create and store a new alert."""
    _, default_priority = ALERT_TYPES.get(alert_type, ("Alerta", "medium"))
    conn = get_connection()
    try:
        conn.execute(
            """INSERT INTO alerts (ticker, alert_type, message, priority)
               VALUES (?, ?, ?, ?)""",
            (ticker, alert_type, message, priority or default_priority)
        )
        conn.commit()
        logger.info(f"Alert created: [{alert_type}] {ticker} - {message}")
    finally:
        conn.close()


def get_unread_alerts(limit: int = 50) -> list:
    """Return unread alerts ordered by priority and date."""
    priority_order = "CASE priority WHEN 'critical' THEN 1 WHEN 'high' THEN 2 WHEN 'medium' THEN 3 ELSE 4 END"
    conn = get_connection()
    try:
        rows = conn.execute(
            f"""SELECT * FROM alerts WHERE is_read=0
                ORDER BY {priority_order}, created_at DESC LIMIT ?""",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def get_all_alerts(limit: int = 100) -> list:
    """Return all alerts."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT * FROM alerts ORDER BY created_at DESC LIMIT ?",
            (limit,)
        ).fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def mark_read(alert_id: int):
    """Mark alert as read."""
    conn = get_connection()
    try:
        conn.execute("UPDATE alerts SET is_read=1 WHERE id=?", (alert_id,))
        conn.commit()
    finally:
        conn.close()


def mark_all_read():
    """Mark all alerts as read."""
    conn = get_connection()
    try:
        conn.execute("UPDATE alerts SET is_read=1")
        conn.commit()
    finally:
        conn.close()


def check_portfolio_alerts(decisions: list = None):
    """
    Check open positions and generate alerts.
    Run after each daily update or scan.
    """
    from modules.data_collector import get_ohlcv_df

    positions = get_portfolio()
    profile   = get_user_profile()
    alerts    = []

    for pos in positions:
        ticker      = pos["ticker"]
        qty         = pos["quantity"]
        avg         = pos["avg_price"]
        stop        = pos.get("stop_price")
        target      = pos.get("target_price")
        current     = pos.get("current_price") or avg

        # Stop hit
        if stop and current <= stop:
            msg = (f"Preço atual {current:.2f} cruzou o stop {stop:.2f}. "
                   f"Perda estimada: R$ {(current - avg) * qty:.2f}")
            create_alert(ticker, "stop_hit", msg)
            alerts.append(("stop_hit", ticker))

        # Near stop (within 2%)
        elif stop and current <= stop * 1.02:
            msg = f"Preço {current:.2f} próximo ao stop {stop:.2f} (< 2% de distância)"
            create_alert(ticker, "exit_position", msg, "high")
            alerts.append(("near_stop", ticker))

        # Target reached
        if target and current >= target * 0.97:
            msg = (f"Preço {current:.2f} próximo ao alvo {target:.2f}. "
                   f"Considere realizar parcialmente.")
            create_alert(ticker, "target_reached", msg)
            alerts.append(("target_reached", ticker))

        # Excessive loss (> 2x planned risk)
        pnl_pct = (current - avg) / avg
        if pnl_pct < -(profile.get("risk_per_trade_pct", 1.0) / 100) * 2:
            msg = f"Posição com perda de {pnl_pct:.1%}, acima do dobro do risco planejado."
            create_alert(ticker, "reduce_position", msg)
            alerts.append(("excess_loss", ticker))

    # New opportunities from scan
    if decisions:
        buy_actions = ["comprar_pequeno", "comprar_normal", "pre_compra"]
        for d in decisions:
            if d.get("action") in buy_actions:
                opp_score = d.get("opportunity_score", 0)
                if opp_score >= 60:
                    msg = (f"Nova oportunidade: {d.get('setup_name')}. "
                           f"Score: {opp_score:.0f}/100 | EV: {d.get('expected_value_r', 0):.2f}R")
                    create_alert(d["ticker"], "new_opportunity", msg)

    logger.info(f"Portfolio alerts checked: {len(alerts)} generated")
    return alerts


def check_regime_change(current_regime: str):
    """Alert if regime changed from yesterday."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT regime, date FROM market_regime ORDER BY date DESC LIMIT 2"
        ).fetchall()
        if len(rows) >= 2:
            today_r = rows[0]["regime"]
            prev_r  = rows[1]["regime"]
            if today_r != prev_r:
                msg = f"Regime de mercado mudou de '{prev_r}' para '{today_r}'"
                create_alert(None, "regime_change", msg)
    finally:
        conn.close()
