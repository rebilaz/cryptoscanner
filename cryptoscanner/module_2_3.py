"""Detect anomalies in on-chain data and store alerts."""

from __future__ import annotations


import pandas as pd
from google.cloud import bigquery

from .logger import get_logger
from .bigquery_client import (
    get_client,
    ensure_dataset,
    ensure_table,
    write_dataframe,
    validate_dataframe,
)
from .module_2_2 import run_onchain_indicator_job

LOGGER = get_logger(__name__)

TABLE_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("eth_transferred", "FLOAT"),
    bigquery.SchemaField("anomaly", "BOOL"),
]


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Simple anomaly detection using mean + 2*std threshold."""
    threshold = df["eth_transferred"].mean() + 2 * df["eth_transferred"].std()
    df["anomaly"] = df["eth_transferred"] > threshold
    return df


def run_anomaly_job(project_id: str = "starlit-verve-458814-u9", dataset: str = "cryptoscanner") -> None:
    """Detect anomalies from on-chain data and store alerts in BigQuery."""
    LOGGER.info("Running anomaly detection job")
    client = get_client(project_id)
    ensure_dataset(client, dataset)
    table_id = f"{project_id}.{dataset}.anomaly_alerts_onchain"
    ensure_table(client, table_id, TABLE_SCHEMA)

    df_daily = run_onchain_indicator_job(project_id, dataset)
    df_alerts = detect_anomalies(df_daily)
    df_alerts = validate_dataframe(df_alerts, TABLE_SCHEMA)
    write_dataframe(df_alerts, table_id, client)
    LOGGER.info("Wrote anomaly alerts to %s", table_id)
