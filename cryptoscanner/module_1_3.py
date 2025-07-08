"""Decision engine based on strategy signals."""

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
    bigquery.SchemaField("decision", "STRING"),
    bigquery.SchemaField("timestamp", "TIMESTAMP"),
]


def generate_decisions(signals: pd.DataFrame) -> pd.DataFrame:
    """Generate simplistic decisions based on moving averages.

    Parameters
    ----------
    signals : pd.DataFrame
        Indicator data with ``ma5`` and ``ma20`` columns.

    Returns
    -------
    pd.DataFrame
        DataFrame of decisions per symbol.
    """
    decisions = []
    for _, row in signals.iterrows():
        if row["ma5"] > row["ma20"]:
            decision = "LONG"
        else:
            decision = "SHORT"
        decisions.append({
            "symbol": row["symbol"],
            "decision": decision,
            "timestamp": pd.Timestamp.utcnow(),
        })
    return pd.DataFrame(decisions)


def run_decision_job(project_id: str = "starlit-verve-458814-u9", dataset: str = "cryptoscanner") -> None:
    """Read strategy signals and produce decisions.

    Parameters
    ----------
    project_id : str
        GCP project identifier.
    dataset : str
        BigQuery dataset name.
    """
    client = get_client(project_id)
    ensure_dataset(client, dataset)
    signal_table = f"{project_id}.{dataset}.market_strategy_signals"
    output_table = f"{project_id}.{dataset}.market_decision_outputs"
    ensure_table(client, output_table, TABLE_SCHEMA)

    LOGGER.info("Running decision job")
    df_signals = read_dataframe(signal_table, client)
    df_decisions = generate_decisions(df_signals)
    df_decisions = validate_dataframe(df_decisions, TABLE_SCHEMA)
    write_dataframe(df_decisions, output_table, client)
    LOGGER.info("Wrote decisions to %s", output_table)
