import pandas as pd
from cryptoscanner.module_1_1 import normalize_binance_data


def test_normalize_binance_data():
    data = [{"symbol": "BTCUSDT", "priceChangePercent": "1", "lastPrice": "42000", "closeTime": 1}]
    df = normalize_binance_data(data)
    assert list(df.columns) == ["symbol", "priceChangePercent", "lastPrice", "closeTime"]
    assert not df.empty
