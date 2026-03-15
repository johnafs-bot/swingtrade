"""
Módulo 4 — Universo de Ativos
Define e filtra o universo de ativos da B3 elegíveis para análise.
"""

import logging
import sqlite3
import pandas as pd
from database.connection import get_connection
import config

logger = logging.getLogger(__name__)

# ─── Lista base: Ibovespa + IBRX-100 (principais ações líquidas B3) ──────────
B3_UNIVERSE = {
    # Financeiro
    "ITUB4": ("Itaú Unibanco", "Financeiro", "Bancos"),
    "BBDC4": ("Bradesco",      "Financeiro", "Bancos"),
    "BBAS3": ("Banco do Brasil","Financeiro","Bancos"),
    "SANB11":("Santander",     "Financeiro", "Bancos"),
    "B3SA3": ("B3",            "Financeiro", "Serv. Financeiros"),
    "IRBR3": ("IRB Brasil",    "Financeiro", "Seguros"),
    "SULA11":("Sul América",   "Financeiro", "Seguros"),
    "CIEL3": ("Cielo",         "Financeiro", "Serv. Financeiros"),
    "BBSE3": ("BB Seguridade",  "Financeiro","Seguros"),
    "PSSA3": ("Porto Seguro",   "Financeiro","Seguros"),

    # Petróleo & Gás
    "PETR4": ("Petrobras PN",  "Petróleo e Gás", "Petróleo"),
    "PETR3": ("Petrobras ON",  "Petróleo e Gás", "Petróleo"),
    "PRIO3": ("PetroRio",      "Petróleo e Gás", "Petróleo"),
    "RECV3": ("PetroReconcavo","Petróleo e Gás", "Petróleo"),
    "RRRP3": ("3R Petroleum",  "Petróleo e Gás", "Petróleo"),
    "UGPA3": ("Ultrapar",      "Petróleo e Gás", "Distribuição"),
    "CSAN3": ("Cosan",         "Petróleo e Gás", "Distribuição"),
    "VBBR3": ("Vibra",         "Petróleo e Gás", "Distribuição"),

    # Mineração & Siderurgia
    "VALE3": ("Vale",          "Materiais Básicos", "Mineração"),
    "CSNA3": ("CSN",           "Materiais Básicos", "Siderurgia"),
    "GGBR4": ("Gerdau PN",     "Materiais Básicos", "Siderurgia"),
    "GOAU4": ("Metal. Gerdau", "Materiais Básicos", "Siderurgia"),
    "USIM5": ("Usiminas",      "Materiais Básicos", "Siderurgia"),
    "CMIN3": ("CSN Mineração", "Materiais Básicos", "Mineração"),

    # Energia Elétrica
    "ELET3": ("Eletrobras ON", "Utilidade Pública", "Energia Elétrica"),
    "ELET6": ("Eletrobras PNB","Utilidade Pública", "Energia Elétrica"),
    "CPFE3": ("CPFL Energia",  "Utilidade Pública", "Energia Elétrica"),
    "EGIE3": ("Engie Brasil",  "Utilidade Pública", "Energia Elétrica"),
    "ENGI11":("Energisa",      "Utilidade Pública", "Energia Elétrica"),
    "TAEE11":("Taesa",         "Utilidade Pública", "Energia Elétrica"),
    "TRPL4": ("CTEEP",         "Utilidade Pública", "Energia Elétrica"),
    "CMIG4": ("Cemig PN",      "Utilidade Pública", "Energia Elétrica"),
    "NEOE3": ("Neoenergia",    "Utilidade Pública", "Energia Elétrica"),
    "AURE3": ("Auren Energia", "Utilidade Pública", "Energia Elétrica"),
    "EQTL3": ("Equatorial",    "Utilidade Pública", "Energia Elétrica"),

    # Varejo & Consumo
    "MGLU3": ("Magazine Luiza","Consumo Cíclico",  "Comércio"),
    "LREN3": ("Lojas Renner",  "Consumo Cíclico",  "Vestuário"),
    "AZZA3": ("Azzas 2154",    "Consumo Cíclico",  "Vestuário"),
    "SOMA3": ("Grupo Soma",    "Consumo Cíclico",  "Vestuário"),
    "AMER3": ("Americanas",    "Consumo Cíclico",  "Comércio"),
    "VVAR3": ("Viavarejo",     "Consumo Cíclico",  "Comércio"),
    "PETZ3": ("Petz",          "Consumo Cíclico",  "Comércio"),
    "SBFG3": ("SBF Grupo",     "Consumo Cíclico",  "Comércio"),
    "LWSA3": ("Locaweb",       "Tecnologia",        "Software"),

    # Alimentos & Bebidas
    "ABEV3": ("Ambev",         "Consumo Não Cíclico","Bebidas"),
    "BEEF3": ("Minerva",       "Consumo Não Cíclico","Alimentos"),
    "MRFG3": ("Marfrig",       "Consumo Não Cíclico","Alimentos"),
    "JBSS3": ("JBS",           "Consumo Não Cíclico","Alimentos"),
    "BRFS3": ("BRF",           "Consumo Não Cíclico","Alimentos"),
    "SMTO3": ("São Martinho",  "Consumo Não Cíclico","Açúcar e Álcool"),
    "SLCE3": ("SLC Agrícola",  "Consumo Não Cíclico","Agropecuária"),
    "AGRO3": ("BrasilAgro",    "Consumo Não Cíclico","Agropecuária"),
    "MATS3": ("Mater Dei",     "Saúde",             "Serv. Médicos"),

    # Saúde
    "HAPV3": ("Hapvida",       "Saúde", "Saúde"),
    "RDOR3": ("Rede D'Or",     "Saúde", "Hospitais"),
    "FLRY3": ("Fleury",        "Saúde", "Diagnóstico"),
    "DASA3": ("Dasa",          "Saúde", "Diagnóstico"),
    "PNVL3": ("Dimed",         "Saúde", "Farmácia"),
    "RAIA3": ("RaiaDrogasil",  "Saúde", "Farmácia"),

    # Construção Civil & Imóveis
    "CYRE3": ("Cyrela",        "Consumo Cíclico", "Construção Civil"),
    "MRVE3": ("MRV",           "Consumo Cíclico", "Construção Civil"),
    "EZTC3": ("EZTEC",         "Consumo Cíclico", "Construção Civil"),
    "EVEN3": ("Even",          "Consumo Cíclico", "Construção Civil"),
    "DIRR3": ("Direcional",    "Consumo Cíclico", "Construção Civil"),
    "PLPL3": ("Plano&Plano",   "Consumo Cíclico", "Construção Civil"),
    "MULT3": ("Multiplan",     "Imóveis",         "Shopping"),
    "BRML3": ("BR Malls",      "Imóveis",         "Shopping"),
    " IGTI11":("Iguatemi",      "Imóveis",         "Shopping"),

    # Logística & Transporte
    "RAIL3": ("Rumo",          "Bens Industriais", "Ferrovias"),
    "CCRO3": ("CCR",           "Bens Industriais", "Rodovias"),
    "ECOR3": ("EcoRodovias",   "Bens Industriais", "Rodovias"),
    "GOLL4": ("Gol",           "Bens Industriais", "Aviação"),
    "AZUL4": ("Azul",          "Bens Industriais", "Aviação"),
    "EMBR3": ("Embraer",       "Bens Industriais", "Aeronáutica"),

    # Tecnologia & Telecom
    "VIVT3": ("Telefônica Vivo","Comunicações",  "Telecom"),
    "TIMS3": ("Tim",           "Comunicações",   "Telecom"),
    "TOTS3": ("Totvs",         "Tecnologia",     "Software"),
    "INTB3": ("Intelbras",     "Tecnologia",     "Hardware"),
    "POSI3": ("Positivo",      "Tecnologia",     "Hardware"),
    "CASH3": ("Méliuz",        "Tecnologia",     "Fintech"),
    "IFCM3": ("Infracommerce", "Tecnologia",     "E-commerce"),

    # Papel & Celulose
    "KLBN11":("Klabin",        "Materiais Básicos","Papel e Celulose"),
    "SUZB3": ("Suzano",        "Materiais Básicos","Papel e Celulose"),

    # Química & Fertilizantes
    "UNIP6": ("Unipar",        "Materiais Básicos","Química"),
    "BRKM5": ("Braskem",       "Materiais Básicos","Petroquímica"),

    # Educação
    "YDUQ3": ("Yduqs",         "Consumo Cíclico", "Educação"),
    "COGN3": ("Cogna",         "Consumo Cíclico", "Educação"),
    "ANIM3": ("Anima",         "Consumo Cíclico", "Educação"),

    # ETFs e benchmark
    "BOVA11":("iShares Ibovespa ETF","ETF","Índice"),
    "SMAL11":("iShares Small Cap ETF","ETF","Índice"),
}


def seed_assets():
    """Insert all tickers into assets table if not present."""
    conn = get_connection()
    try:
        cur = conn.cursor()
        for ticker, (name, sector, subsector) in B3_UNIVERSE.items():
            cur.execute(
                """INSERT OR IGNORE INTO assets (ticker, name, sector, subsector)
                   VALUES (?, ?, ?, ?)""",
                (ticker, name, sector, subsector)
            )
        conn.commit()
        logger.info(f"Seeded {len(B3_UNIVERSE)} assets.")
    finally:
        conn.close()


def get_eligible_tickers(
    min_volume: float = config.MIN_VOLUME_DAILY_BRL,
    min_price: float = config.MIN_PRICE,
    min_history_days: int = config.MIN_HISTORY_DAYS,
    exclude_sectors: list = None,
    exclude_types: list = None,
) -> list[str]:
    """
    Return list of tickers that pass liquidity, price and history filters.
    Uses the last 30 days of OHLCV to compute average daily volume (R$).
    """
    conn = get_connection()
    try:
        # Average volume over last 30 trading days
        query = """
        WITH vol_stats AS (
            SELECT
                o.ticker,
                COUNT(o.date) AS days,
                AVG(o.close * o.volume) AS avg_vol_brl,
                MAX(o.close) AS last_price
            FROM ohlcv o
            JOIN assets a ON o.ticker = a.ticker
            WHERE o.date >= date('now', '-45 days')
              AND a.is_active = 1
            GROUP BY o.ticker
        )
        SELECT ticker FROM vol_stats
        WHERE avg_vol_brl >= ?
          AND last_price >= ?
          AND days >= 15
        """
        rows = conn.execute(query, (min_volume, min_price)).fetchall()
        liquid = {r["ticker"] for r in rows}

        # History filter — need at least min_history_days of data
        hist_query = """
        SELECT ticker, COUNT(date) AS cnt
        FROM ohlcv GROUP BY ticker
        HAVING cnt >= ?
        """
        hist_rows = conn.execute(hist_query, (min_history_days,)).fetchall()
        sufficient = {r["ticker"] for r in hist_rows}

        eligible = liquid & sufficient

        if exclude_sectors:
            sec_query = "SELECT ticker FROM assets WHERE sector IN ({})".format(
                ",".join("?" * len(exclude_sectors))
            )
            exc = {r["ticker"] for r in conn.execute(sec_query, exclude_sectors).fetchall()}
            eligible -= exc

        if exclude_types:
            type_query = "SELECT ticker FROM assets WHERE asset_type IN ({})".format(
                ",".join("?" * len(exclude_types))
            )
            exc = {r["ticker"] for r in conn.execute(type_query, exclude_types).fetchall()}
            eligible -= exc

        result = sorted(eligible)
        logger.info(f"Eligible tickers: {len(result)}")
        return result

    finally:
        conn.close()


def get_all_tickers() -> list[str]:
    """Return all active tickers from assets table."""
    conn = get_connection()
    try:
        rows = conn.execute(
            "SELECT ticker FROM assets WHERE is_active=1 ORDER BY ticker"
        ).fetchall()
        return [r["ticker"] for r in rows]
    finally:
        conn.close()


def get_asset_info(ticker: str) -> dict:
    """Return asset metadata."""
    conn = get_connection()
    try:
        row = conn.execute(
            "SELECT * FROM assets WHERE ticker=?", (ticker,)
        ).fetchone()
        return dict(row) if row else {}
    finally:
        conn.close()
