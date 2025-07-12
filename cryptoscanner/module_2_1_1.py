import logging
from google.cloud import bigquery
import pandas as pd

from cryptoscanner.bigquery_client import write_dataframe

logger = logging.getLogger(__name__)

# Remplace {project_id} par ton vrai ID de projet GCP ou importe-le depuis une config/env
TABLE_ID = "starlit-verve-458814-u9.cryptoscanner.onchain_raw_metrics"

# Exemple de requête sur les transactions Ethereum
QUERY = """
    SELECT
        block_timestamp AS timestamp,
        from_address AS address,
        value / 1e18 AS eth_transferred,
        gas_price / 1e9 AS gas_price_gwei,
        "ethereum" AS source
    FROM
        `bigquery-public-data.crypto_ethereum.transactions`
    WHERE
        block_timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 DAY)
    LIMIT 10000
"""

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

def ingest_onchain_bigquery_to_bq():
    """
    Ingest on-chain data from BigQuery public dataset and write to BigQuery table.
    All decimal.Decimal columns are converted to float, and all bytes columns (e.g. addresses)
    are converted to hexadecimal strings for BigQuery compatibility.
    """
    logger.info("Fetching on-chain data from BigQuery public dataset")
    client = bigquery.Client()

    query_job = client.query(QUERY)
    df = query_job.to_dataframe()
    # Convert decimal.Decimal columns to float before any computation or writing
    df = convert_decimal_to_float(df)
    # Convert bytes columns (e.g. addresses) to hex string for BigQuery compatibility
    for col in df.columns:
        if df[col].dtype == 'object':
            df[col] = df[col].apply(lambda x: x.hex() if isinstance(x, bytes) else x)
    # Ensure address columns are string type
    df = ensure_str_columns(df, ["address"])

    logger.info(f"Writing {len(df)} rows to {TABLE_ID}")
    write_dataframe(df, TABLE_ID, client)
    logger.info(f"On-chain data written to {TABLE_ID}")
