import json
import unittest
from datetime import datetime
from io import StringIO
from typing import Any
from unittest.mock import MagicMock, patch

import pandas as pd

from scraper import download_jao, download_pse, join_both, load_api_key, process_jao_json, process_pse_csv


class TestJaoFunctions(unittest.TestCase):

    def test_load_api_key_returns_key(self) -> None:
        with patch("builtins.open", MagicMock(return_value=StringIO("test_api_key"))):
            api_key = load_api_key("test_file")
            self.assertEqual(api_key, "test_api_key")

    def test_load_api_key_raises_value_error_when_file_is_empty(self) -> None:
        with patch("builtins.open", MagicMock(return_value=StringIO(""))):
            with self.assertRaises(IndexError):
                load_api_key("test_file")

    def test_load_api_key_raises_error_when_file_does_not_exist(self) -> None:
        with patch("builtins.open", MagicMock(side_effect=FileNotFoundError)):
            with self.assertRaises(FileNotFoundError):
                load_api_key("non_existing_file")

    def test_download_pse(self) -> None:
        mock_response = MagicMock(status_code=200, content=b"test_csv_data")
        with patch("requests.get", return_value=mock_response):
            out_path = download_pse(
                start_date=datetime(2022, 1, 1), 
                end_date=datetime(2022, 1, 2),
                out_path="tests/test_PSE.csv",
            )
            self.assertTrue(out_path.exists())

        mock_response.status_code = 404
        with patch("requests.get", return_value=mock_response):
            with self.assertRaises(Exception):
                download_pse(out_path="tests/test_PSE.csv")

    def test_download_jao(self) -> None:
        mock_response = MagicMock(status_code=200, content=b"test_csv_data", json={"test": "json_data"})
        with patch("requests.get", return_value=mock_response):
            out_path = download_jao(api_key="test_api_key", out_path="tests/test_JAO.json")
            self.assertTrue(out_path.exists())

        mock_response.status_code = 404
        with patch("requests.get", return_value=mock_response):
            with self.assertRaises(Exception):
                download_jao(out_path="tests/test_JAO.json")

    @patch("pandas.DataFrame.to_csv")
    def test_process_jao_json(self, to_csv_mock: Any) -> None:
        test_data = [
            {
                "marketPeriodStart": "2022-01-01T00:00:00",
                "results": [{"productHour": "01:00-02:00"}],
            }
        ]
        with patch(
            "builtins.open", MagicMock(return_value=StringIO(json.dumps(test_data)))
        ):
            df = process_jao_json()
            self.assertIsInstance(df, pd.DataFrame)

    @patch("pandas.DataFrame.to_csv")
    def test_process_pse_csv(self, to_csv_mock: Any) -> None:
        with patch("builtins.open", MagicMock(return_value=StringIO("Data;Godzina\n2022-01-01;1"))):
            df = process_pse_csv()
            self.assertIsInstance(df, pd.DataFrame)

    def test_join_both(self) -> None:
        df_left = pd.DataFrame({"datetime": ["2022-01-01 01:00"], "data_left": [1]})
        df_right = pd.DataFrame({"datetime": ["2022-01-01 01:00"], "data_right": [2]})
        with patch("pathlib.Path.exists", return_value=True):
            out_df = join_both(df_left, df_right, out_path="tests/test_output.csv")
            self.assertIsInstance(out_df, pd.DataFrame)
            self.assertTrue(
                out_df.equals(
                    pd.DataFrame(
                        {
                            "datetime": ["2022-01-01 01:00"],
                            "data_left": [1],
                            "data_right": [2],
                        }
                    )
                )
            )

        with patch("pathlib.Path.exists", return_value=False):
            out_df = join_both(df_left, df_right, out_path="tests/test_output.csv")
            self.assertIsInstance(out_df, pd.DataFrame)
            self.assertTrue(
                out_df.equals(
                    pd.DataFrame(
                        {
                            "datetime": ["2022-01-01 01:00"],
                            "data_left": [1],
                            "data_right": [2],
                        }
                    )
                )
            )
