"""Execute the full CryptoScanner pipeline."""
from cryptoscanner import (
    ingest_binance_to_bq,
    run_indicator_job,
    run_decision_job,
    ingest_dune_to_bq,
    run_anomaly_job,
    alert_from_bigquery,
)
from cryptoscanner.logger import get_logger

LOGGER = get_logger(__name__)


def main() -> None:
    LOGGER.info("Starting CryptoScanner pipeline")
    ingest_binance_to_bq()
    run_indicator_job()
    run_decision_job()
    try:
        ingest_dune_to_bq(query_id="12345")  # Example query id
        run_anomaly_job()
    except Exception as exc:
        LOGGER.error("On-chain modules failed: %s", exc)
    alert_from_bigquery()
    LOGGER.info("Pipeline finished")


if __name__ == "__main__":
    main()
