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


def send_telegram_messages(api_token: str, chat_id: str, messages: Iterable[str]) -> None:
    """Send each message to Telegram and log any failure."""
    bot = Bot(token=api_token)
    for msg in messages:
        try:
            bot.send_message(chat_id=chat_id, text=msg, parse_mode="Markdown")
        except Exception as exc:
            LOGGER.error("Failed to send message: %s", exc)


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
