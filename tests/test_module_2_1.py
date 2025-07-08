from cryptoscanner.module_2_1 import normalize_dune_data
import pandas as pd


def test_normalize_dune_data():
    data = [{"metric": "m", "value": 1, "time": "2024-01-01"}]
    df = normalize_dune_data(data)
    assert "timestamp" in df.columns
