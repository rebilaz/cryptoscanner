"""Send alerts and decisions via Telegram."""

from __future__ import annotations

import os
from typing import Iterable

from google.cloud import bigquery
from telegram import Bot

from .logger import get_logger
from .bigquery_client import get_client, read_dataframe

LOGGER = get_logger(__name__)


def fetch_messages(table_id: str, client: bigquery.Client) -> Iterable[str]:
    """Yield serialized messages from a BigQuery table."""
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
    if hasattr(telegram, 'Bot') and hasattr(telegram.Bot, 'send_message') and telegram.__version__.startswith('20'):
        ASYNC_TELEGRAM = True
    else:
        ASYNC_TELEGRAM = False
except ImportError:
    ASYNC_TELEGRAM = False


def send_telegram_messages(api_token: str, chat_id: str, messages: list) -> None:
    """
    Send a single summary message to Telegram. All values are made serializable. Async if v20+, else sync.
    Only one message is sent per run, summarizing the results (e.g. number of signals/anomalies/incidents).
    This avoids spamming and respects Telegram API limits.
    """
    # Compose a single summary message
    n_signals = len(messages)
    n_anomalies = sum('anomaly' in str(m).lower() for m in messages)
    n_incidents = sum('incident' in str(m).lower() for m in messages)
    summary = f"\U0001F4CA Scan terminé ! {n_signals} signaux, {n_anomalies} anomalies, {n_incidents} incident(s) on-chain."
    summary = ensure_serializable(summary)
    if ASYNC_TELEGRAM:
        import asyncio
        from telegram import Bot
        async def send_async():
            bot = Bot(token=api_token)
            await bot.send_message(chat_id=chat_id, text=summary, parse_mode="Markdown")
        asyncio.run(send_async())
    else:
        from telegram import Bot
        bot = Bot(token=api_token)
        bot.send_message(chat_id=chat_id, text=summary, parse_mode="Markdown")


def alert_from_bigquery(
    api_token: str | None = None,
    chat_id: str | None = None,
    project_id: str = "starlit-verve-458814-u9",
    dataset: str = "cryptoscanner",
) -> None:
    """Send decision and anomaly alerts via Telegram.

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

    messages = list(fetch_messages(decision_table, client)) + list(fetch_messages(anomaly_table, client))
    send_telegram_messages(api_token, chat_id, messages)
    LOGGER.info("Sent %d messages to Telegram", len(messages))
