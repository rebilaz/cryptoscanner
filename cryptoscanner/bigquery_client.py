"""Utility functions to interact with BigQuery.

This module provides helper functions to create a connection using
Application Default Credentials (ADC), ensure a dataset and table exist,
and read/write Pandas DataFrames to BigQuery.
"""

from __future__ import annotations

from .logger import get_logger
from typing import Optional
import os

from google.cloud import bigquery
import pandas as pd

LOGGER = get_logger(__name__)

DEFAULT_PROJECT = "starlit-verve-458814-u9"
DEFAULT_DATASET = "cryptoscanner"


def get_client(project_id: str = DEFAULT_PROJECT) -> bigquery.Client:
    """Return a BigQuery client using Application Default Credentials.

    Any ``GOOGLE_APPLICATION_CREDENTIALS`` environment variable will be
    ignored to enforce IAM-based authentication.
    """
    LOGGER.debug("Creating BigQuery client for project %s", project_id)
    os.environ.pop("GOOGLE_APPLICATION_CREDENTIALS", None)
    return bigquery.Client(project=project_id)


def ensure_dataset(client: bigquery.Client, dataset_name: str = DEFAULT_DATASET) -> bigquery.Dataset:
    """Create the dataset if it does not exist and return it."""
    dataset_id = f"{client.project}.{dataset_name}"
    try:
        dataset = client.get_dataset(dataset_id)
        LOGGER.debug("Dataset %s already exists", dataset_id)
    except Exception:
        LOGGER.info("Creating dataset %s", dataset_id)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = "US"
        dataset = client.create_dataset(dataset, exists_ok=True)
    return dataset


def ensure_table(
    client: bigquery.Client,
    table_id: str,
    schema: list[bigquery.SchemaField],
) -> bigquery.Table:
    """Create a table if it does not exist and return it."""
    try:
        table = client.get_table(table_id)
        LOGGER.debug("Table %s already exists", table_id)
    except Exception:
        LOGGER.info("Creating table %s", table_id)
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
    return table


def validate_dataframe(df: pd.DataFrame, schema: list[bigquery.SchemaField]) -> pd.DataFrame:
    """Validate DataFrame columns and NaN values against BigQuery schema."""
    expected = [field.name for field in schema]
    missing = [col for col in expected if col not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")
    if df[expected].isnull().any().any():
        raise ValueError("DataFrame contains NaN values")
    return df[expected]


def write_dataframe(
    df: pd.DataFrame,
    table_id: str,
    client: Optional[bigquery.Client] = None,
    if_exists: str = "append",
) -> None:
    """Write a DataFrame to BigQuery."""
    client = client or get_client()
    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    if if_exists == "replace":
        job_config.write_disposition = "WRITE_TRUNCATE"
    LOGGER.info("Writing %d rows to %s", len(df), table_id)
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()


def read_dataframe(table_id: str, client: Optional[bigquery.Client] = None) -> pd.DataFrame:
    """Read a table from BigQuery into a DataFrame."""
    client = client or get_client()
    LOGGER.debug("Reading table %s", table_id)
    query = f"SELECT * FROM `{table_id}`"
    return client.query(query).to_dataframe()
