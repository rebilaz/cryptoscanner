import logging
from google.cloud import bigquery
import pandas as pd

from cryptoscanner.bigquery_client import write_dataframe

logger = logging.getLogger(__name__)

# Remplace {project_id} par ton vrai ID de projet GCP ou importe-le depuis une config/env
TABLE_ID = "starlit-verve-458814-u9.cryptoscanner.market_raw_metrics"

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

def ingest_onchain_bigquery_to_bq():
    logger.info("Fetching on-chain data from BigQuery public dataset")
    client = bigquery.Client()

    query_job = client.query(QUERY)
    df = query_job.to_dataframe()

    logger.info(f"Writing {len(df)} rows to {TABLE_ID}")
    write_dataframe(df, TABLE_ID, client)

    logger.info(f"On-chain data written to {TABLE_ID}")
