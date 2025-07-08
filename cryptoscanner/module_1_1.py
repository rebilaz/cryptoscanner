"""Module for ingesting market data from CEX APIs into BigQuery."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import requests
from google.cloud import bigquery

from .logger import get_logger
from .bigquery_client import (
    get_client,
    ensure_dataset,
    ensure_table,
    write_dataframe,
    validate_dataframe,
)

LOGGER = get_logger(__name__)

BINANCE_ENDPOINT = "https://api.binance.com/api/v3/ticker/24hr"


TABLE_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("priceChangePercent", "FLOAT"),
    bigquery.SchemaField("lastPrice", "FLOAT"),
    bigquery.SchemaField("closeTime", "TIMESTAMP"),
]

def fetch_binance_ticker() -> List[Dict[str, Any]]:
    """Fetch 24hr ticker data from Binance.

    Returns
    -------
    List[Dict[str, Any]]
        Raw ticker rows returned by the API.
    """
    LOGGER.info("Fetching data from Binance")
    try:
        resp = requests.get(BINANCE_ENDPOINT, timeout=10)
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        LOGGER.error("Error fetching Binance data: %s", exc)
        raise
    LOGGER.debug("Received %d rows", len(data))
    return data


def normalize_binance_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Normalize raw Binance data into a DataFrame.

    Parameters
    ----------
    data : List[Dict[str, Any]]
        Raw ticker rows from Binance.

    Returns
    -------
    pd.DataFrame
        Normalized DataFrame with selected columns.
    """
    df = pd.DataFrame(data)
    df = df[["symbol", "priceChangePercent", "lastPrice", "closeTime"]]
    df["closeTime"] = pd.to_datetime(df["closeTime"], unit="ms")
    return df


def ingest_binance_to_bq(project_id: str = "starlit-verve-458814-u9", dataset: str = "cryptoscanner") -> None:
    """Ingest Binance ticker data into BigQuery.

    Parameters
    ----------
    project_id : str
        GCP project identifier.
    dataset : str
        BigQuery dataset name.
    """
    client = get_client(project_id)
    ensure_dataset(client, dataset)
    table_id = f"{project_id}.{dataset}.market_raw_metrics"
    ensure_table(client, table_id, TABLE_SCHEMA)

    raw = fetch_binance_ticker()
    df = normalize_binance_data(raw)
    df = validate_dataframe(df, TABLE_SCHEMA)
    write_dataframe(df, table_id, client)
    LOGGER.info("Ingested %d rows into %s", len(df), table_id)
