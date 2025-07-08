import pandas as pd
from cryptoscanner.module_2_3 import detect_anomalies


def test_detect_anomalies():
    df = pd.DataFrame({"date": ["2024-01-01"], "value": [10]})
    result = detect_anomalies(df)
    assert "anomaly" in result.columns
