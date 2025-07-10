from dotenv import load_dotenv
load_dotenv()

"""Execute the full CryptoScanner pipeline."""
from cryptoscanner import (
    ingest_binance_to_bq,
    run_indicator_job,
    run_decision_job,
    run_anomaly_job,
    alert_from_bigquery,
)
from cryptoscanner.module_2_1_1 import ingest_onchain_bigquery_to_bq
from cryptoscanner.logger import get_logger

LOGGER = get_logger(__name__)

def main() -> None:
    LOGGER.info("Starting CryptoScanner pipeline")
    ingest_binance_to_bq()
    run_indicator_job()
    run_decision_job()
    try:
        ingest_onchain_bigquery_to_bq()  # Remplace le module Dune 2.1
        run_anomaly_job()
    except Exception as exc:
        LOGGER.error("On-chain modules failed: %s", exc)
    alert_from_bigquery()
    LOGGER.info("Pipeline finished")

if __name__ == "__main__":
    main()
