import pandas as pd
from cryptoscanner.module_1_2 import compute_moving_averages


def test_compute_moving_averages():
    df = pd.DataFrame({"symbol": ["BTC"], "lastPrice": [1], "closeTime": [pd.Timestamp('2024-01-01')]})
    result = compute_moving_averages(df)
    assert "ma5" in result.columns
