import json
import logging
import pathlib
from argparse import ArgumentParser
from datetime import datetime, timedelta
from http import HTTPStatus
from typing import Optional, Union

import pandas as pd
import requests

logging.basicConfig(
    encoding='utf-8',
    format='[%(asctime)s] %(levelname)s:%(message)s',
    level=logging.DEBUG,
    handlers=[logging.FileHandler("scraper.log"), logging.StreamHandler()],
)


def load_api_key(
    filepath: Union[pathlib.Path, str] = ".JAO_API_KEY",
) -> str:
    """Load JAO API KEY (token) from text file
    Reference: https://www.jao.eu/get-token

    :param filepath: str or pathlib.Path to text file with API KEY (token)
    :return: str with API KEY (token)
    """
    try:
        filepath = pathlib.Path(filepath).resolve()
        with open(filepath, "r") as src:
            api_key = src.readlines()
            return api_key[0].split()[0]
    except IndexError:
        raise IndexError(f"Place valid JAO API KEY (token) into {filepath}. Reference https://www.jao.eu/get-token")
    except FileNotFoundError:
        raise FileNotFoundError(f"File {filepath} not found. Create it and paste JAO API KEY (token) there. Reference https://www.jao.eu/get-token")


def download_pse(
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    out_path: Optional[str] = None,
) -> Optional[pathlib.Path]:
    """Download PSE tabular data as CSV file from last 30 days or defined period.
    Reference: https://www.pse.pl/dane-systemowe/plany-pracy-kse/biezacy-plan-koordynacyjny-dobowy-bpkd/wielkosci-podstawowe

    :param start_date: start of reporting period
    :param end_date: end of reporting period
    :param out_path: path to create or overwrite csv file
    :return: path to saved csv table
    """
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(30)
    end_date_str = end_date.strftime("%Y%m%d")
    start_date_str = start_date.strftime("%Y%m%d")
    csv_url = f"https://www.pse.pl/getcsv/-/export/csv/PL_BPKD/data_od/{start_date_str}/data_do/{end_date_str}"
    if not out_path:
        outdir = pathlib.Path("./downloads").resolve()
        outdir.mkdir(exist_ok=True, parents=True)
        out_path = outdir.joinpath("PSE.csv")
    with open(out_path, "wb") as dest:
        logging.info(f"Requesting {csv_url} \nNOTE: this step could take several minutes")
        response = requests.get(csv_url)
        if response.status_code == HTTPStatus.OK:
            dest.write(response.content)
            return pathlib.Path(out_path).resolve()
        raise requests.exceptions.RequestException(f"Unexpected response status code: {response.status_code}")


def download_jao(
    api_key: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None,
    out_path: Optional[pathlib.Path] = None,
) -> Optional[pathlib.Path]:
    """Download auctions data from JAO api for last 30 days or custom period
    not longer than 31 days. Check references for additional info.
    Reference for token: https://www.jao.eu/get-token
    Reference for API: https://www.jao.eu/page-api/market-data

    :param api_key: valid API KEY (token)
    :param start_date: start of reporting period
    :param end_date: end of reporting period
    :param out_path: path to create or overwrite JSON file
    :return: path to saved JSON file
    """
    if not api_key:
        api_key = load_api_key()
    if not end_date:
        end_date = datetime.now()
    if not start_date:
        start_date = end_date - timedelta(30)
    end_date_str = end_date.strftime("%Y-%m-%d")
    start_date_str = start_date.strftime("%Y-%m-%d")
    if not out_path:
        outdir = pathlib.Path("./downloads").resolve()
        outdir.mkdir(exist_ok=True, parents=True)
        out_path = outdir.joinpath("JAO.json")
    with open(out_path, "wb") as dest:
        logging.info(f"Requesting https://api.jao.eu/OWSMP/getauctions")
        response = requests.get(
            url="https://api.jao.eu/OWSMP/getauctions",
            headers={"AUTH_API_KEY": api_key},
            params={
                "corridor": "PL-UA",
                "fromdate": f"{start_date_str}-00:00:00",
                "todate": f"{end_date_str}-23:59:59",
                "horizon": "daily",
            },
        )
        if response.status_code == HTTPStatus.OK:
            dest.write(response.content)
            return pathlib.Path(out_path).resolve()
        raise requests.exceptions.RequestException(f"Unexpected response status code: {response.status_code}")


def process_jao_json(
    filepath: Union[pathlib.Path, str] = "./downloads/JAO.json",
) -> pd.DataFrame:
    """Flatten JSON file with JAO auction reports and store it as new csv file.

    :param filepath: path to JSON file with unmodified JAO auctions report
    :return: DataFrame with auctions as rows with datetime labels
    """
    filepath = pathlib.Path(filepath).resolve()
    with open(filepath, "r") as json_file:
        json_data = json.load(json_file)
    flattened = []
    for day in json_data:
        date_str = day["marketPeriodStart"][:10]
        for hour in day["results"]:
            hour["datetime"] = datetime.strptime(
                f"{date_str} {hour['productHour'][:5]}", "%Y-%m-%d %H:%M"
            )
            flattened.append(hour)
    df = pd.DataFrame(flattened)
    out_path = filepath.parent.joinpath(filepath.stem + "_modified.csv")
    df.to_csv(f"{out_path}")
    return df


def process_pse_csv(
    filepath: Union[pathlib.Path, str] = "./downloads/PSE.csv",
) -> pd.DataFrame:
    """Process downloaded PSE report to match encoding and ISO timestamps

    :param filepath: path to downloaded csv file of PSE report
    :return: DataFrame with rows labeled by timestamps
    """
    filepath = pathlib.Path(filepath).resolve()
    df = pd.read_csv(filepath, encoding="CP1250", sep=";")
    df["datetime"] = (
        df["Data"] + " " + df["Godzina"].apply(lambda x: str(x).zfill(2)) + ":00"
    )
    df["datetime"] = df["datetime"].str.replace("24:00", "00:00")
    df["datetime"] = pd.to_datetime(df["datetime"])
    out_path = filepath.parent.joinpath(filepath.stem + "_modified.csv")
    df.to_csv(f"{out_path}")
    return df


def join_both(
    df_left: pd.DataFrame, 
    df_right: pd.DataFrame, 
    out_path: Optional[str] = None,
) -> Optional[pd.DataFrame]:
    """Join PSE and JAO reports by datetime label

    :param df_left: DataFrame with PSE report or JAO if decided as base
    :param df_right: DataFrame with JAO report or PSE if decided as joined
    :param out_path: path to save result report
    :return: DataFrame with joined report
    """
    df_left["datetime"] = df_left["datetime"].astype(str)
    df_right["datetime"] = df_right["datetime"].astype(str)
    df_merged = pd.merge(df_left, df_right, on="datetime")
    df_merged = df_merged.loc[:, ~df_merged.columns.str.startswith("Unnamed")]
    if not out_path:
        outdir = pathlib.Path("./results")
        outdir.mkdir(parents=True, exist_ok=True)
        out_path = outdir.joinpath("JOINED.csv").resolve()
    df_merged.to_csv(f"{out_path}")
    if pathlib.Path(out_path).exists():
        logging.info(f"Saved joined report table to {out_path} file.")
    return df_merged


def main() -> None:
    parser = ArgumentParser(
        description="Download and merge PSE and JAO reports from last 30 days."
    )
    parser.add_argument(
        "-k",
        "--api_key",
        help="JAO API KEY. When using this argument token from .JAO_API_KEY file will be ignored.",
        type=str,
        required=False,
        default=None,
    )
    parser.add_argument(
        "-o",
        "--out",
        help="Custom output path for report CSV file (e.g. ~/myreport.csv)",
        type=str,
        required=False,
        default=None,
    )
    args = parser.parse_args()

    try:
        jao_json_path = download_jao(api_key=args.api_key)
        jao_df = process_jao_json(jao_json_path)
        pse_csv_path = download_pse()
        pse_df = process_pse_csv(pse_csv_path)
        join_both(pse_df, jao_df, args.out)
    except (IndexError, FileNotFoundError, requests.exceptions.RequestException) as e:
        logging.error(f"Error occurred: {e}")


if __name__ == "__main__":
    main()
