import pandas as pd
from cryptoscanner.module_2_3 import detect_anomalies


def test_detect_anomalies():
    df = pd.DataFrame({
        "date": ["2024-01-01"],
        "eth_transferred": [10.0],
        "gas_price_gwei": [5.0],
    })
    result = detect_anomalies(df)
    assert "anomaly_eth_transferred" in result.columns
    assert "anomaly_gas_price" in result.columns
