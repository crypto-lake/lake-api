from typing import List, Dict, Optional
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
import datetime

import boto3
import botocore
import awswrangler._utils
import pandas as pd
from cachetools_ext.fs import FSLRUCache
from botocache.botocache import botocache_context

import lakeapi._read_parquet

cache = FSLRUCache(ttl=8 * 60 * 60, path="cache/boto", maxsize=1000)
default_bucket = 'qnt.data/market-data/cryptofeed'


def set_default_bucket(bucket: str) -> None:
    global default_bucket
    default_bucket = bucket

def use_sample_data(anonymous_access: bool) -> None:
    '''
    Use sample data lake configuration, which is free for testing Lake.

    :param anonymous_access: Whether to enable anonymous AWS access, that can be used without AWS credentials.
    '''
    set_default_bucket('sample.crypto.lake')

    old_default_config = awswrangler._utils.default_botocore_config
    def _anonymous_access_config() -> None:
        config = old_default_config()
        config.signature_version = botocore.UNSIGNED
        return config

    if anonymous_access:
        awswrangler._utils.default_botocore_config = _anonymous_access_config

def load_data(
    table: Literal["book", "trades", "candles"],
    start: Optional[datetime.datetime] = None,
    end: Optional[datetime.datetime] = None,
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    *,
    bucket: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    use_threads: bool = True,
    columns: Optional[List[str]] = None,
    row_slice: Optional[slice] = None,
    drop_partition_cols: bool = False,
) -> pd.DataFrame:
    '''
    Load data from Lake into Pandas DataFrame.

    Fetches data from a range of exchanges/symbols/dates and returns them as a Pandas DataFrame. All network access
    is cached into a `cache` directory, which is created in the working directory.
    '''
    # TODO: document params
    if end is None:
        end = datetime.datetime.now()
    if bucket is None:
        bucket = default_bucket
    if boto3_session is None:
        boto3_session = boto3.Session(region_name="eu-west-1")

    def partition_filter(partition: Dict[str, str]) -> bool:
        return (
            (
                start is None
                or start.date() <= datetime.date.fromisoformat(partition["dt"])
            )
            and (
                end is None or end.date() > datetime.date.fromisoformat(partition["dt"])
            )
            and (symbols is None or partition["symbol"] in symbols)
            and (exchanges is None or partition["exchange"] in exchanges)
        )

    if symbols:
        assert symbols[0].upper() == symbols[0]
    if exchanges:
        assert exchanges[0].upper() == exchanges[0]

    with botocache_context(
        cache=cache,
        action_regex_to_cache=["List.*"],
        # This helps in logging all calls made to AWS. Useful while debugging. Default value is False.
        call_log=True,
        # This supresses warning messages encountered while caching. Default value is False.
        supress_warning_message=False,
    ):
        # TODO: log & skip corrupted files
        df = lakeapi._read_parquet.read_parquet(
            path=f"s3://{bucket}/{table}",
            partition_filter=partition_filter,
            categories=["side"] if table == "trades" else None,
            dataset=True,  # also adds partition columns
            boto3_session=boto3_session,
            columns=columns,
            use_threads=use_threads,
            ignore_index=True,
        )
    if drop_partition_cols:
        # useful when loading just one symbol and exchange
        df.drop(columns=["symbol", "exchange", "dt"], inplace=True)
    else:
        # dt is contained in time columns
        df.drop(columns=["dt"], inplace=True)
    if row_slice:
        df = df.iloc[row_slice]

    # For compatibility
    if "amount" in df.columns:
        df.rename(columns={"amount": "quantity"}, inplace=True)
    if "receipt_timestamp" in df.columns:
        df.rename(columns={"receipt_timestamp": "received_time"}, inplace=True)
        df["received_time"] = pd.to_datetime(df["received_time"], unit="ns", cache=True)
    if "timestamp" in df.columns:
        df.rename(columns={"timestamp": "origin_time"}, inplace=True)
        df["origin_time"] = pd.to_datetime(df["origin_time"], unit="ns", cache=True)
    if table == "trades":
        df.rename(columns={"id": "trade_id"}, inplace=True)
    return df


if __name__ == "__main__":
    # Test
    # df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 3), end = None, symbols = ['BTC-USDT'], exchanges = ['BINANCE']) # noqa
    # df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 2), end = None, symbols = None, exchanges = ['BINANCE']) # noqa
    df = load_data(
        table="book",
        start=datetime.datetime.now() - datetime.timedelta(days=2),
        end=None,
        symbols=["FRONT-BUSD"],
        exchanges=None,
    )
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_columns", 30)
    print(df)
    # print(df.sample(20))
    print(df.dtypes)
    print(df.memory_usage().sum() / 1e6, "MB")
