"""Send alerts and decisions via Telegram, compatible python-telegram-bot v13."""

import os
import logging
from typing import Iterable

import pandas as pd

from google.cloud import bigquery
from telegram import Bot

from .logger import get_logger
from .bigquery_client import get_client, read_dataframe

LOGGER = get_logger(__name__)

def fetch_messages(table_id: str, client: bigquery.Client) -> Iterable[str]:
    LOGGER.info(f"Fetching messages from table {table_id}")
    try:
        df = read_dataframe(table_id, client)
        LOGGER.info(f"Fetched {len(df)} rows from {table_id}")
        for _, row in df.iterrows():
            yield str(row.to_dict())
    except Exception as e:
        LOGGER.error(f"Error reading {table_id}: {e}")
        raise

def ensure_serializable(obj):
    import decimal
    if isinstance(obj, dict):
        return {k: ensure_serializable(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [ensure_serializable(x) for x in obj]
    elif isinstance(obj, decimal.Decimal):
        return float(obj)
    elif isinstance(obj, bytes):
        return obj.decode('utf-8')
    else:
        return obj

def send_telegram_message(api_token: str, chat_id: str, text: str) -> None:
    text = ensure_serializable(text)
    safe_token = api_token[:8] + "..."  # Masque le token dans les logs
    safe_chat = str(chat_id)[:8] + "..." if isinstance(chat_id, str) else chat_id
    LOGGER.info(f"[TELEGRAM v13] Will send to token={safe_token}, chat_id={safe_chat}")
    try:
        bot = Bot(token=api_token)
        bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")
        LOGGER.info("Telegram message sent successfully.")
    except Exception as e:
        LOGGER.error(f"Erreur envoi Telegram: {e}")

def build_summary_from_dfs(df_decisions: pd.DataFrame, df_anomalies: pd.DataFrame) -> str:
    n_signals = len(df_decisions)
    n_anomalies = len(df_anomalies)
    bool_cols = [c for c in df_anomalies.columns if "anomaly" in c.lower()]
    n_incidents = int(df_anomalies[bool_cols].any(axis=1).sum()) if bool_cols else n_anomalies
    summary = (
        f"\U0001F4CA Scan terminé ! {n_signals} signaux, "
        f"{n_anomalies} anomalies, {n_incidents} incident(s) on-chain."
    )
    LOGGER.info(f"Summary built: {summary}")
    return summary

def build_summary(
    decision_table: str,
    anomaly_table: str,
    client: bigquery.Client,
) -> str:
    try:
        LOGGER.info("Fetching decisions table for summary.")
        df_decisions = read_dataframe(decision_table, client)
        LOGGER.info(f"Decisions: {len(df_decisions)} rows.")
    except Exception as e:
        LOGGER.error(f"Could not fetch decisions table: {e}")
        df_decisions = pd.DataFrame()
    try:
        LOGGER.info("Fetching anomalies table for summary.")
        df_anomalies = read_dataframe(anomaly_table, client)
        LOGGER.info(f"Anomalies: {len(df_anomalies)} rows.")
    except Exception as e:
        LOGGER.error(f"Could not fetch anomalies table: {e}")
        df_anomalies = pd.DataFrame()
    return build_summary_from_dfs(df_decisions, df_anomalies)

def alert_from_bigquery(
    api_token: str | None = None,
    chat_id: str | None = None,
    project_id: str = "starlit-verve-458814-u9",
    dataset: str = "cryptoscanner",
) -> None:
    LOGGER.info("Starting Telegram alert_from_bigquery. [v13 sync]")
    api_token = api_token or os.getenv("TELEGRAM_TOKEN")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    safe_token = api_token[:8] + "..." if api_token else "NONE"
    safe_chat = str(chat_id)[:8] + "..." if chat_id else "NONE"
    LOGGER.info(f"[DEBUG] api_token={safe_token}, chat_id={safe_chat}")
    if not api_token or not chat_id:
        LOGGER.error("Telegram credentials are required")
        raise ValueError("Telegram credentials are required")

    try:
        client = get_client(project_id)
        decision_table = f"{project_id}.{dataset}.market_decision_outputs"
        anomaly_table = f"{project_id}.{dataset}.anomaly_alerts_onchain"
        LOGGER.info(f"Will summarize tables: {decision_table}, {anomaly_table}")
        summary = build_summary(decision_table, anomaly_table, client)
        LOGGER.info(f"Summary ready: {summary}")
        send_telegram_message(api_token, chat_id, summary)
        LOGGER.info("Sent Telegram summary and finished alert_from_bigquery.")
    except Exception as e:
        LOGGER.error(f"Error in alert_from_bigquery: {e}")

