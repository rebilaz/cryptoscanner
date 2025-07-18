"""Compute on-chain indicators from raw metrics."""

from __future__ import annotations

import pandas as pd
from google.cloud import bigquery

from .logger import get_logger
from .bigquery_client import get_client, read_dataframe, ensure_dataset

LOGGER = get_logger(__name__)


def compute_daily_aggregate(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate metrics daily.

    Parameters
    ----------
    df : pd.DataFrame
        Raw on-chain metrics with ``timestamp`` and ``eth_transferred``.

    Returns
    -------
    pd.DataFrame
        Aggregated ETH transferred per day.
    """
    df["date"] = pd.to_datetime(df["timestamp"]).dt.date
    return df.groupby("date").agg({"eth_transferred": "sum"}).reset_index()


def run_onchain_indicator_job(project_id: str = "starlit-verve-458814-u9", dataset: str = "cryptoscanner") -> pd.DataFrame:
    """Read raw on-chain metrics and compute daily aggregates.

    Parameters
    ----------
    project_id : str
        GCP project identifier.
    dataset : str
        BigQuery dataset name.

    Returns
    -------
    pd.DataFrame
        Daily aggregated ETH transferred.
    """
    LOGGER.info("Running on-chain indicator job")
    client = get_client(project_id)
    ensure_dataset(client, dataset)
    raw_table = f"{project_id}.{dataset}.onchain_raw_metrics"

    df_raw = read_dataframe(raw_table, client)
    # S'assurer que les colonnes nécessaires sont présentes
    required_cols = ["timestamp", "address", "eth_transferred", "gas_price_gwei", "source"]
    missing_cols = [col for col in required_cols if col not in df_raw.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in onchain_raw_metrics: {missing_cols}")
    df_daily = compute_daily_aggregate(df_raw)
    return df_daily
