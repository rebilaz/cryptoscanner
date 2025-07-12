"""Send alerts and decisions via Telegram."""

from __future__ import annotations

import os
from typing import Iterable

import pandas as pd

from google.cloud import bigquery
from telegram import Bot

from .logger import get_logger
from .bigquery_client import get_client, read_dataframe

LOGGER = get_logger(__name__)


def fetch_messages(table_id: str, client: bigquery.Client) -> Iterable[str]:
    """Yield serialized messages from a BigQuery table.

    This utility remains for backward compatibility with older tests. It simply
    returns the stringified rows of the table.
    """
    df = read_dataframe(table_id, client)
    for _, row in df.iterrows():
        yield str(row.to_dict())


# NOTE: On n'envoie qu'un message global pour éviter le spam et respecter les limites Telegram.
# On convertit tous les types pour la compatibilité Telegram (Decimal, bytes, etc.).
def ensure_serializable(obj):
    """Recursively convert decimal.Decimal to float and bytes to str for Telegram serialization."""
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


# Compatibilité python-telegram-bot v20+ (async/await) ou v13.15 (sync)
try:
    import telegram
    if telegram.__version__.startswith("20"):
        ASYNC_TELEGRAM = True
    else:
        ASYNC_TELEGRAM = False
except Exception:
    ASYNC_TELEGRAM = False


def send_telegram_message(api_token: str, chat_id: str, text: str) -> None:
    """Send ``text`` to Telegram, handling sync and async APIs transparently."""
    text = ensure_serializable(text)
    if ASYNC_TELEGRAM:
        import asyncio
        async def send_async() -> None:
            bot = Bot(token=api_token)
            await bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")

        asyncio.run(send_async())
    else:
        bot = Bot(token=api_token)
        bot.send_message(chat_id=chat_id, text=text, parse_mode="Markdown")


def build_summary_from_dfs(df_decisions: pd.DataFrame, df_anomalies: pd.DataFrame) -> str:
    """Return a human friendly summary message based on two DataFrames."""
    n_signals = len(df_decisions)
    n_anomalies = len(df_anomalies)
    bool_cols = [c for c in df_anomalies.columns if "anomaly" in c.lower()]
    n_incidents = int(df_anomalies[bool_cols].any(axis=1).sum()) if bool_cols else n_anomalies
    return (
        f"\U0001F4CA Scan terminé ! {n_signals} signaux, "
        f"{n_anomalies} anomalies, {n_incidents} incident(s) on-chain."
    )


def build_summary(
    decision_table: str,
    anomaly_table: str,
    client: bigquery.Client,
) -> str:
    """Fetch data from BigQuery tables and build the summary message."""
    df_decisions = read_dataframe(decision_table, client)
    df_anomalies = read_dataframe(anomaly_table, client)
    return build_summary_from_dfs(df_decisions, df_anomalies)


def alert_from_bigquery(
    api_token: str | None = None,
    chat_id: str | None = None,
    project_id: str = "starlit-verve-458814-u9",
    dataset: str = "cryptoscanner",
) -> None:
    """Send a single Telegram summary of decisions and anomalies.

    Parameters
    ----------
    api_token : str, optional
        Telegram bot token. Defaults to ``TELEGRAM_TOKEN`` env var.
    chat_id : str, optional
        Telegram chat identifier. Defaults to ``TELEGRAM_CHAT_ID`` env var.
    """
    api_token = api_token or os.getenv("TELEGRAM_TOKEN")
    chat_id = chat_id or os.getenv("TELEGRAM_CHAT_ID")
    if not api_token or not chat_id:
        raise ValueError("Telegram credentials are required")

    client = get_client(project_id)
    decision_table = f"{project_id}.{dataset}.market_decision_outputs"
    anomaly_table = f"{project_id}.{dataset}.anomaly_alerts_onchain"

    summary = build_summary(decision_table, anomaly_table, client)
    send_telegram_message(api_token, chat_id, summary)
    LOGGER.info("Sent Telegram summary: %s", summary)
