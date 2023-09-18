from typing import List, Dict, Optional, Any
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
import datetime

import boto3
import botocore
import botocore.exceptions
import awswrangler._utils
import awswrangler as wr
import pandas as pd
from cachetools_ext.fs import FSLRUCache
from botocache.botocache import botocache_context

import lakeapi._read_parquet
import lakeapi._cache

DataType = Literal["book", "book_delta", "trades", "trades_mpid", "candles", "level_1", "funding", "open_interest", "liquiditions"]

cache = FSLRUCache(ttl=8 * 60 * 60, path=".lake_cache/boto", maxsize=10_000)
default_bucket = 'qnt.data/market-data/cryptofeed'
is_anonymous_access = False


def set_default_bucket(bucket: str) -> None:
    global default_bucket
    default_bucket = bucket

def set_cache_size_limit(limit_bytes: int) -> None:
    '''
    Set cache size limit in bytes.

    :param limit_bytes: Cache size limit in bytes.
    '''
    lakeapi._cache._store.bytes_limit = limit_bytes

def use_sample_data(anonymous_access: bool) -> None:
    '''
    Use sample data lake configuration, which is free for testing Lake.

    :param anonymous_access: Whether to enable anonymous AWS access, that can be used without AWS credentials.
    '''
    global is_anonymous_access
    set_default_bucket('sample.crypto.lake')

    old_default_config = awswrangler._utils.default_botocore_config
    def _anonymous_access_config() -> None:
        config = old_default_config()
        config.signature_version = botocore.UNSIGNED
        return config

    if anonymous_access:
        is_anonymous_access = True
        awswrangler._utils.default_botocore_config = _anonymous_access_config

def load_data(
    table: DataType,
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
        # This supresses warning messages encountered while caching in anonymous mode
        supress_warning_message=is_anonymous_access,
    ):
        last_ex = None
        for _ in range(2):
            try:
                # TODO: log & skip corrupted files
                df = lakeapi._read_parquet.read_parquet(
                    path=f"s3://{bucket}/{table}/",
                    partition_filter=partition_filter,
                    categories=["side"] if table == "trades" else None,
                    dataset=True,  # also adds partition columns
                    boto3_session=boto3_session,
                    columns=columns,
                    use_threads=use_threads,
                    ignore_index=True,
                )
                break
            except botocore.exceptions.ClientError as ex:
                # When 404 file not found error happens, it means the boto cache of available files is wrong and we need
                # to clear it and try again.
                if int(ex.response['Error']['Code']) == 404:
                    # An error occurred (404) when calling the HeadObject operation: Not Found
                    cache.clear()
                    last_ex = ex
                    continue
                else:
                    raise
        else:
            # got error 404 both before and after the cache.clear()
            raise last_ex
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
    if "next_funding_time" in df.columns:
        df["next_funding_time"] = pd.to_datetime(df["next_funding_time"], unit="s", cache=True)
    if table == "trades":
        df.rename(columns={"id": "trade_id"}, inplace=True)

    lakeapi._cache._store.reduce_size()
    return df

def list_data(
    table: Optional[DataType],
    start: Optional[datetime.datetime] = None,
    end: Optional[datetime.datetime] = None,
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    *,
    bucket: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
) -> List[Dict[str, str]]:
    '''
    Returns list of all data s3 objects matching given conditions.

    Elements describing s3 objects are dicts containing keys table, exchange, symbol, dt, filename.
    '''
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

    path = f"s3://{bucket}/{table}/"
    with botocache_context(
        cache=cache,
        action_regex_to_cache=["List.*"],
        # This helps in logging all calls made to AWS. Useful while debugging. Default value is False.
        call_log=True,
        # This supresses warning messages encountered while caching. Default value is False.
        supress_warning_message=False,
    ):
        paths = lakeapi._read_parquet._path2list(
            path=path,
            boto3_session=boto3_session,
            # suffix=path_suffix,
            # ignore_suffix=_get_path_ignore_suffix(path_ignore_suffix=path_ignore_suffix),
            last_modified_begin=last_modified_begin,
            last_modified_end=last_modified_end,
            # ignore_empty=ignore_empty,
            s3_additional_kwargs={},
        )
        path_root = lakeapi._read_parquet._get_path_root(path=path, dataset=True)
    paths = lakeapi._read_parquet._apply_partition_filter(path_root=path_root, paths=paths, filter_func=partition_filter)
    if len(paths) < 1:
        raise wr.exceptions.NoFilesFound(f"No files Found on: {path}.")
    return [_path_to_dict(path) for path in paths]

def _path_to_dict(path: str) -> Dict[str, Any]:
    *_, table, exchange, symbol, dt, filename = path.split('/')
    return {
        'table': table,
        'exchange': exchange.lstrip('exchange='),
        'symbol': symbol.lstrip('symbol='),
        'dt': dt.lstrip('dt='),
        'filename': filename,
    }

def available_symbols(
    table: Optional[DataType],
    exchanges: Optional[List[str]] = None,
    *,
    bucket: Optional[str] = None,
    boto3_session: Optional[boto3.Session] = None,
):
    '''
    Return pd.Series containing count of days available for exchange-symbol combinations.

    Contains multi-index of exchange, symbol pairs.
    '''
    objects = list_data(table = table, exchanges = exchanges, bucket = bucket, boto3_session = boto3_session)
    df = pd.DataFrame(objects)
    counts = df.groupby(['exchange', 'symbol']).filename.count()
    counts.name = 'days_available'
    return counts.sort_values(ascending = False)


if __name__ == "__main__":
    # Test
    # df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 3), end = None, symbols = ['BTC-USDT'], exchanges = ['BINANCE']) # noqa
    # df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 2), end = None, symbols = None, exchanges = ['BINANCE']) # noqa
    use_sample_data(True)
    df = load_data(
        table="book",
        start=None, #datetime.datetime.now() - datetime.timedelta(days=2),
        end=None,
        symbols=["FTRB-USDT"],
        exchanges=None,
    )
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_columns", 30)
    print(df)
    # print(df.sample(20))
    print(df.dtypes)
    print(df.memory_usage().sum() / 1e6, "MB")
