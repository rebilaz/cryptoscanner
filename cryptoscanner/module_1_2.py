"""Compute strategy indicators from market raw metrics."""

from __future__ import annotations

from typing import List

import pandas as pd
from google.cloud import bigquery

from .logger import get_logger
from .bigquery_client import (
    get_client,
    read_dataframe,
    ensure_table,
    ensure_dataset,
    write_dataframe,
    validate_dataframe,
)

LOGGER = get_logger(__name__)

TABLE_SCHEMA = [
    bigquery.SchemaField("symbol", "STRING"),
    bigquery.SchemaField("ma5", "FLOAT"),
    bigquery.SchemaField("ma20", "FLOAT"),
]


def compute_moving_averages(df: pd.DataFrame) -> pd.DataFrame:
    """Compute simple moving averages as an example indicator.

    Parameters
    ----------
    df : pd.DataFrame
        Raw metrics with ``lastPrice`` and ``closeTime`` columns.

    Returns
    -------
    pd.DataFrame
        DataFrame containing ``symbol``, ``ma5`` and ``ma20`` columns.
    """
    df = df.sort_values("closeTime")
    df["ma5"] = df["lastPrice"].rolling(window=5).mean()
    df["ma20"] = df["lastPrice"].rolling(window=20).mean()
    return df[["symbol", "ma5", "ma20"]].dropna()


def run_indicator_job(project_id: str = "starlit-verve-458814-u9", dataset: str = "cryptoscanner") -> None:
    """Read raw metrics, compute indicators and store results.

    Parameters
    ----------
    project_id : str
        GCP project identifier.
    dataset : str
        BigQuery dataset name.
    """
    LOGGER.info("Running indicator job")
    client = get_client(project_id)
    ensure_dataset(client, dataset)
    raw_table = f"{project_id}.{dataset}.market_raw_metrics"
    signal_table = f"{project_id}.{dataset}.market_strategy_signals"
    ensure_table(client, signal_table, TABLE_SCHEMA)

    df_raw = read_dataframe(raw_table, client)
    df_indicators = compute_moving_averages(df_raw)
    df_indicators = validate_dataframe(df_indicators, TABLE_SCHEMA)
    write_dataframe(df_indicators, signal_table, client)
    LOGGER.info("Wrote strategy signals to %s", signal_table)
