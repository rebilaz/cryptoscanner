import pandas as pd
from cryptoscanner.module_2_2 import compute_daily_aggregate


def test_compute_daily_aggregate():
    df = pd.DataFrame({"timestamp": [pd.Timestamp('2024-01-01')], "eth_transferred": [1]})
    out = compute_daily_aggregate(df)
    assert out.iloc[0]["eth_transferred"] == 1
