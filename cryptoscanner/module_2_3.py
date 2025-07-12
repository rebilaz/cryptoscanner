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
    read_dataframe,
)
from .module_2_2 import run_onchain_indicator_job

LOGGER = get_logger(__name__)

TABLE_SCHEMA = [
    bigquery.SchemaField("date", "DATE"),
    bigquery.SchemaField("eth_transferred", "FLOAT"),
    bigquery.SchemaField("gas_price_gwei", "FLOAT"),
    bigquery.SchemaField("anomaly_eth_transferred", "BOOL"),
    bigquery.SchemaField("anomaly_gas_price", "BOOL"),
]


def detect_anomalies(df: pd.DataFrame) -> pd.DataFrame:
    """Detect anomalies on eth_transferred and gas_price_gwei using mean + 2*std threshold per day."""
    # Anomalie sur eth_transferred
    eth_threshold = df["eth_transferred"].mean() + 2 * df["eth_transferred"].std()
    df["anomaly_eth_transferred"] = df["eth_transferred"] > eth_threshold
    # Anomalie sur gas_price_gwei
    gas_threshold = df["gas_price_gwei"].mean() + 2 * df["gas_price_gwei"].std()
    df["anomaly_gas_price"] = df["gas_price_gwei"] > gas_threshold
    return df


def convert_decimal_to_float(df):
    """Convert all decimal.Decimal columns in a DataFrame to float for BigQuery/serialization compatibility."""
    import decimal
    for col in df.columns:
        if df[col].dtype == 'object' and any(isinstance(x, decimal.Decimal) for x in df[col]):
            df[col] = df[col].apply(lambda x: float(x) if isinstance(x, decimal.Decimal) else x)
    return df


def ensure_str_columns(df, columns):
    """Ensure columns like address/hash are string type for BigQuery and pandas compatibility."""
    for col in columns:
        if col in df.columns:
            df[col] = df[col].astype(str)
    return df


def run_anomaly_job(project_id: str = "starlit-verve-458814-u9", dataset: str = "cryptoscanner") -> None:
    """Detect anomalies from on-chain data and store alerts in BigQuery."""
    LOGGER.info("Running anomaly detection job")
    client = get_client(project_id)
    ensure_dataset(client, dataset)
    table_id = f"{project_id}.{dataset}.anomaly_alerts_onchain"
    ensure_table(client, table_id, TABLE_SCHEMA)

    # Lire la table onchain_raw_metrics
    raw_table = f"{project_id}.{dataset}.onchain_raw_metrics"
    df_raw = read_dataframe(raw_table, client)
    # Convert decimal.Decimal columns to float before any computation or writing
    df_raw = convert_decimal_to_float(df_raw)
    # Ensure address columns are string type
    df_raw = ensure_str_columns(df_raw, ["address"])
    # S'assurer que les colonnes nécessaires sont présentes
    required_cols = ["timestamp", "address", "eth_transferred", "gas_price_gwei", "source"]
    missing_cols = [col for col in required_cols if col not in df_raw.columns]
    if missing_cols:
        raise ValueError(f"Missing columns in onchain_raw_metrics: {missing_cols}")
    # Agréger par jour
    df_raw["date"] = pd.to_datetime(df_raw["timestamp"]).dt.date
    df_daily = df_raw.groupby("date").agg({
        "eth_transferred": "sum",
        "gas_price_gwei": "mean"
    }).reset_index()
    df_alerts = detect_anomalies(df_daily)
    df_alerts = validate_dataframe(df_alerts, TABLE_SCHEMA)
    write_dataframe(df_alerts, table_id, client)
    LOGGER.info("Wrote anomaly alerts to %s", table_id)
