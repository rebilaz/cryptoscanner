from cryptoscanner.module_3_1 import fetch_messages
import pandas as pd
from unittest.mock import MagicMock


def test_fetch_messages():
    client = MagicMock()
    client.query.return_value.to_dataframe.return_value = pd.DataFrame({"a": [1]})
    msgs = list(fetch_messages("table", client))
    assert msgs
