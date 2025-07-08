import pandas as pd
from cryptoscanner.module_1_3 import generate_decisions


def test_generate_decisions():
    df = pd.DataFrame({"symbol": ["BTC"], "ma5": [2], "ma20": [1]})
    decisions = generate_decisions(df)
    assert decisions.iloc[0]["decision"] == "LONG"
