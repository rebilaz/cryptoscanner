from cryptoscanner.module_3_1 import fetch_messages, build_summary_from_dfs
import pandas as pd
from unittest.mock import MagicMock


def test_fetch_messages():
    client = MagicMock()
    client.query.return_value.to_dataframe.return_value = pd.DataFrame({"a": [1]})
    msgs = list(fetch_messages("table", client))
    assert msgs


def test_build_summary_from_dfs():
    df_dec = pd.DataFrame({"decision": ["LONG", "SHORT"]})
    df_anom = pd.DataFrame({
        "anomaly_eth_transferred": [True, False],
        "anomaly_gas_price": [False, True],
    })
    summary = build_summary_from_dfs(df_dec, df_anom)
    assert "2 signaux" in summary
    assert "2 anomalies" in summary
