# CryptoScanner

CryptoScanner is a modular crypto market analysis pipeline built around Google BigQuery. The project collects market data from centralized exchanges and on‑chain sources, computes strategy indicators, produces trading decisions and sends Telegram alerts.

## Repository structure

```
cryptoscanner/
├── cryptoscanner/
│   ├── bigquery_client.py
│   ├── logger.py
│   ├── module_1_1.py  # CEX ingestion
│   ├── module_1_2.py  # CEX indicators
│   ├── module_1_3.py  # Decision engine
│   ├── module_2_1.py  # Dune ingestion
│   ├── module_2_1_1.py  # Public BigQuery ingestion
│   ├── module_2_2.py  # On‑chain indicators
│   ├── module_2_3.py  # Anomaly detection
│   └── module_3_1.py  # Telegram alerting
├── tests/
├── run_pipeline.py
├── requirements.txt
└── .env.example
```

## Installation

1. Clone the repository and create a virtual environment:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

2. Copy `.env.example` to `.env` and fill in the variables:

- `DUNE_API_KEY` – API key for Dune Analytics
- `TELEGRAM_TOKEN` and `TELEGRAM_CHAT_ID` – Telegram bot credentials
- `LOG_LEVEL` – logging level (INFO by default)

3. Ensure the IAM identity running the code has `BigQuery Data Editor` on the
   dataset `cryptoscanner` and `BigQuery Job User` on the project
   `starlit-verve-458814-u9`.

## BigQuery authentication

The pipeline relies on [Application Default Credentials](https://cloud.google.com/docs/authentication/production#automatically) (ADC).
No service account JSON file is required. When running on AWS or other
infrastructure, configure Workload Identity Federation or an instance profile so
that `google-cloud-bigquery` can obtain credentials automatically. Any
`GOOGLE_APPLICATION_CREDENTIALS` environment variable will be ignored.

## Running the pipeline

The example script `run_pipeline.py` executes the full workflow:

```bash
python run_pipeline.py
```

A typical Telegram alert message looks like:

```
🚀 *BTC* decision: LONG
```

## Tests

Run all unit tests with `pytest`:

```bash
pytest
```

## Cloud deployment notes

The modules are self‑contained functions and can be orchestrated with cron jobs or container schedulers (Docker/Kubernetes). For secret management, use GCP Secret Manager or AWS Secrets Manager and export the values to the environment before running the pipeline.

# 1. Active ton environnement virtuel (si pas déjà fait)
source .venv/bin/activate

# 2. Lance le pipeline principal
python run_pipeline.py