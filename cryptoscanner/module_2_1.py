"""Ingest on-chain data using Dune Analytics API."""

from __future__ import annotations

import os
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

DUNE_ENDPOINT = "https://api.dune.com/api/v1/query/"  # placeholder

TABLE_SCHEMA = [
    bigquery.SchemaField("metric", "STRING"),
    bigquery.SchemaField("value", "FLOAT"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
]

API_KEY_HEADER = "X-Dune-API-Key"


def fetch_dune_data(query_id: str, api_key: str) -> List[Dict[str, Any]]:
    """Fetch data from Dune Analytics.

    Parameters
    ----------
    query_id : str
        Identifier of the Dune query.
    api_key : str
        Dune API key.

    Returns
    -------
    list of dict
        Raw rows from Dune.
    """
    url = f"{DUNE_ENDPOINT}{query_id}/results"
    LOGGER.info("Fetching data from Dune query %s", query_id)
    headers = {API_KEY_HEADER: api_key}
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json().get("result", {}).get("rows", [])
    except Exception as exc:
        LOGGER.error("Error fetching Dune data: %s", exc)
        raise
    LOGGER.debug("Received %d rows", len(data))
    return data


def normalize_dune_data(data: List[Dict[str, Any]]) -> pd.DataFrame:
    """Normalize Dune query result rows.

    Parameters
    ----------
    data : list of dict
        Raw rows from Dune.

    Returns
    -------
    pd.DataFrame
        DataFrame with ``timestamp`` column.
    """
    df = pd.DataFrame(data)
    return df.rename(columns={"time": "timestamp"})


def ingest_dune_to_bq(
    query_id: str,
    api_key: str | None = None,
    project_id: str = "starlit-verve-458814-u9",
    dataset: str = "cryptoscanner",
) -> None:
    """Ingest Dune Analytics data into BigQuery.

    Parameters
    ----------
    query_id : str
        Identifier of the Dune query.
    api_key : str, optional
        API key to authenticate. Falls back to ``DUNE_API_KEY`` env var.
    project_id : str
        GCP project identifier.
    dataset : str
        BigQuery dataset name.
    """
    api_key = api_key or os.getenv("DUNE_API_KEY")
    if not api_key:
        raise ValueError("Dune API key is required")

    client = get_client(project_id)
    ensure_dataset(client, dataset)
    table_id = f"{project_id}.{dataset}.market_raw_metrics"
    ensure_table(client, table_id, TABLE_SCHEMA)

    raw = fetch_dune_data(query_id, api_key)
    df = normalize_dune_data(raw)
    df = validate_dataframe(df, TABLE_SCHEMA)
    write_dataframe(df, table_id, client)
    LOGGER.info("Ingested %d rows from Dune into %s", len(df), table_id)
