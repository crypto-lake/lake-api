from typing import List, Dict, Optional, Any, Tuple
try:
    from typing import Literal
except ImportError:
    from typing_extensions import Literal
import datetime
import os
import requests
import functools
import io
import json
import warnings
import zlib

import boto3
import botocore
import botocore.exceptions
import pandas as pd
import pandas.errors
from cachetools_ext.fs import FSLRUCache
from botocache.botocache import botocache_context
from aws_requests_auth.aws_auth import AWSRequestsAuth
import tqdm.contrib.concurrent
import cachetools
import joblib.memory
import pyarrow

import lakeapi._read_parquet
import lakeapi._cache
import lakeapi._utils
import lakeapi.exceptions

DataType = Literal[
    "book", "book_delta", "trades", "trades_mpid", "candles", "level_1", "funding", "open_interest", "liquidations",
    "book_1m"
]

cache = FSLRUCache(ttl=8 * 60 * 60, path=".lake_cache/boto", maxsize=10_000)
default_bucket = 'qnt.data/market-data/cryptofeed'
is_anonymous_access = False
_old_default_config = None


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
    global is_anonymous_access, _old_default_config
    set_default_bucket('sample.crypto.lake')

    if _old_default_config is None:
        _old_default_config = lakeapi._utils.default_botocore_config
    def _anonymous_access_config() -> None:
        config = _old_default_config()
        config.signature_version = botocore.UNSIGNED
        return config

    if anonymous_access:
        is_anonymous_access = True
        lakeapi._utils.default_botocore_config = _anonymous_access_config
    else:
        is_anonymous_access = False
        lakeapi._utils.default_botocore_config = _old_default_config


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
    cached: bool = True
) -> pd.DataFrame:
    '''
    Load data from Lake into Pandas DataFrame.

    Fetches data from a range of exchanges/symbols/dates and returns them as a Pandas DataFrame. All network access
    can be cached into a `.lake_cache` directory, which is created in the working directory.

    :param table: Data type to load. Eg. book (2x20 level order book snapshots ), trades, candles, level_1, funding, open_interest and more
    :param start: Start datetime of data to load. If None, loads all data until `end`. Will be rounded to midnight.
    :param end: End datetime of data to load. If None, loads all data from `start`. Will be rounded to midnight.
    :param symbols: List of symbols to load. If None, loads all symbols available. Eg. ['BTC-USDT', 'ETH-USDT']
    :param exchanges: List of exchanges to load. If None, loads all exchanges available. Eg. ['BINANCE', 'BINANCE_FUTURES', KUCOIN']
    :param bucket: S3 bucket to load data from. Reserved for internal usage.
    :param boto3_session: Boto3 session to use for loading data. Usually left None = create a new session.
    :param use_threads: Whether to use multiple threads for loading data for better performance.
    :param columns: List of columns to load. If None, loads all columns available.
    :param row_slice: DEPRECATED
    :param drop_partition_cols: Whether to drop columns (dt, symbol and exchange) from the DataFrame. Useful when loading just one symbol and exchange.
    :param cached: Whether to use file system cache for data download. However, always uses cache for file listing.
    '''
    if end is None:
        end = datetime.datetime.now()
    if bucket is None:
        bucket = default_bucket
    if boto3_session is None:
        boto3_session = boto3.Session(region_name="eu-west-1")
    username, method = _login(boto3_session, table)
    # print(username, method)
    if method == 'upgrade':
        warnings.warn('This lakeapi version is outdated and might misbehave. Please upgrade lakeapi to the latest version eg. using command `pip install -U lakeapi`!')
    if method == 'forceupgrade':
        raise Exception('This lakeapi version is outdated. Please upgrade lakeapi to the latest version eg. using command `pip install -U lakeapi`!')

    def partition_filter(partition: Dict[str, str]) -> bool:
        return (
            "dt" in partition
            and (
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
        assert symbols[0].upper() == symbols[0] or symbols[0][5:].upper() == symbols[0][5:]
    if exchanges:
        assert exchanges[0].upper() == exchanges[0]

    if method == 'cloudfront':
        df = _load_data_cloudfront(
            table = table, start = start, end = end, symbols = symbols, exchanges = exchanges,
            boto3_session = boto3_session, use_threads = use_threads, username = username, cached = cached,
        )
    else:
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
                    df = lakeapi._read_parquet.read_parquet(
                        path=f"s3://{bucket}/{table}/",
                        partition_filter=partition_filter,
                        categories=["side"] if table == "trades" else None,
                        dataset=True,  # also adds partition columns
                        boto3_session=boto3_session,
                        columns=columns,
                        use_threads=use_threads,
                        ignore_index=True,
                        cached=cached,
                    )
                    break
                except botocore.exceptions.ClientError as ex:
                    # When 404 file not found error happens, it means the boto cache of available files is wrong and we need
                    # to clear it and try again.
                    if int(ex.response['Error']['Code']) == 404:
                        # An error occurred (404) when calling the HeadObject operation: Not Found
                        cache.clear()
                        contents_cache.clear()
                        last_ex = ex
                        continue
                    else:
                        raise
                except lakeapi.exceptions.NoFilesFound:
                    if is_anonymous_access:
                        raise lakeapi.exceptions.NoFilesFound("No data found for your query in the free sample dataset. Please subscribe to access more data.")
                    else:
                        raise
                except botocore.exceptions.UnauthorizedSSOTokenError as ex:
                    raise PermissionError(
                        "It seems you use a AWS account and try to access free data. Try using "
                        "`use_sample_data(anonymous_access=False)` or read https://github.com/crypto-lake/lake-api/issues/7."
                    )
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
        try:
            df["next_funding_time"] = pd.to_datetime(df["next_funding_time"], unit="s", cache=True)
        except pandas.errors.OutOfBoundsDatetime:
            df["next_funding_time"] = pd.to_datetime(df["next_funding_time"], unit="ns", cache=True)
    if table == "trades":
        df.rename(columns={"id": "trade_id"}, inplace=True)

    lakeapi._cache._store.reduce_size()
    return df


@cachetools.cached(cache=FSLRUCache(maxsize=32, path = '.lake_cache/login', ttl=3600), key=lambda sess, table: f'table={table}')
def _login(boto3_session: boto3.Session, table: str) -> Tuple[str, str]:
    ''' return username and method for download '''
    lambda_client = boto3_session.client('lambda')
    try:
        response = lambda_client.invoke(
            FunctionName='lake-login-login',
            InvocationType='RequestResponse',
            Payload=json.dumps({
                'command': 'login.login',
                'api_key': boto3_session.get_credentials().access_key,
                'table': table,
                'anonymous_access': is_anonymous_access,
                'user_agent': f'lakeapi/{lakeapi.__version__}'
            }),
        )

        # Read the response from the Lambda function
        response_payload = json.loads(response['Payload'].read().decode('utf-8'))

        return response_payload['username'], response_payload['method']
    except:
        return 'unknown', 's3'


def _load_data_cloudfront(
    table: DataType,
    start: Optional[datetime.datetime] = None,
    end: Optional[datetime.datetime] = None,
    symbols: Optional[List[str]] = None,
    exchanges: Optional[List[str]] = None,
    *,
    boto3_session: Optional[boto3.Session] = None,
    use_threads: bool = True,
    username: str = 'unknown',
    cached: bool = True,
    # drop_partition_cols: bool = False,
) -> pd.DataFrame:
    if boto3_session is None:
        boto3_session = boto3.Session(region_name="eu-west-1")

    # credentials = boto3_session.get_credentials().get_frozen_credentials()
    # auth = AWSRequestsAuth(
    #     aws_access_key=credentials.access_key,
    #     aws_secret_access_key=credentials.secret_key,
    #     aws_host='s3.amazonaws.com',
    #     aws_region='eu-west-1',
    #     aws_service='lambda'
    # )
    # login_response = requests.get(login_api_url, auth = auth, params={'api_key': credentials.access_key})
    # try:
    # username = _login(boto3_session)
    # except:
    #     username = 'anonymous'

    # if not exchanges or not symbols or not start or not end:
    available_data = list_data(table = table, start = start, end = end, symbols = symbols, exchanges = exchanges, boto3_session = boto3_session)
    df = pd.DataFrame(available_data)
    if not exchanges:
        exchanges = list(df['exchange'].unique())
    if not symbols:
        symbols = list(df['symbol'].unique())
    if not start:
        start = datetime.datetime.strptime(df['dt'].min(), '%Y-%m-%d')
    if not end:
        end = datetime.datetime.strptime(df['dt'].max(), '%Y-%m-%d')
    # else:
    #     df = None

    assert exchanges and symbols and start and end
    # for start in dateutil.rrule.rrule(freq = dateutil.rrule.DAILY, dtstart = start.date(), until = end.date()):
    #     for exchange in exchanges:
    #         for symbol in symbols:
    #             url = f'https://data.crypto-lake.com/market-data/cryptofeed/{table}/exchange={exchange}/symbol={symbol}/dt={start.date()}/1.snappy.parquet'

    objs = []
    for row in df.itertuples():
        url = f'https://data.crypto-lake.com/market-data/cryptofeed/{table}/exchange={row.exchange}/symbol={row.symbol}/dt={row.dt}/{row.filename}'
        objs.append((url, row.symbol, row.exchange, row.dt))

    if use_threads and os.cpu_count():
        workers = os.cpu_count() + 2
    else:
        workers = 1

    dfs = list(tqdm.contrib.concurrent.thread_map(
        functools.partial(_download_cloudfront, boto3_session, username, symbols, exchanges, cached),
        objs,
        max_workers=workers
    ))
    return pd.concat([df for df in dfs if not df.empty], ignore_index=True)


def _download_cloudfront(session: boto3.Session, username: str, all_symbols: List[str], all_exchanges: List[str], cached: bool, obj) -> pd.DataFrame:
    url, symbol, exchange, dt = obj
    credentials = session.get_credentials().get_frozen_credentials()
    auth = AWSRequestsAuth(
        aws_access_key=credentials.access_key,
        aws_secret_access_key=credentials.secret_key,
        aws_host='qnt.data.s3.amazonaws.com',
        aws_region='eu-west-1',
        aws_service='s3'
    )
    if cached:
        with warnings.catch_warnings():
            # Ignore joblib warnings for:
            # - duplicate function _download_cached. This is caused by ipython according to github
            # - slow persist arguments. No idea why this sometimes happens, but we pass no big arguments.
            warnings.simplefilter("ignore", UserWarning)
            df = _download_cached(auth, url, username)
    else:
        df = _download_one(auth, url, username)
    if 'side' in df.columns:
        df['side'] = pd.Series(df['side'], index=df.index, dtype=pd.CategoricalDtype(categories = ['buy', 'sell']))
    df['symbol'] = pd.Series(symbol, index=df.index, dtype=pd.CategoricalDtype(categories = all_symbols))
    df['exchange'] = pd.Series(exchange, index=df.index, dtype=pd.CategoricalDtype(categories = all_exchanges))
    df['dt'] = 0 # this will be deleted anyway
    return df

def _download_one(auth, url: str, username: str) -> pd.DataFrame:
    # Use stream to be able to process response.raw into parquet faster
    # response = requests.get(url, auth = auth, headers = {'Referer': username, 'User-Agent': f'lakeapi/{lakeapi.__version__}'}, stream = True)
    # return pd.read_parquet(lakeapi.response_stream.ResponseStream(response.iter_content(chunk_size=1_000_000)), engine='pyarrow')

    response = requests.get(url, auth = auth, headers = {'Referer': username, 'User-Agent': f'lakeapi/{lakeapi.__version__}'})

    if response.status_code == 404:
        contents_cache.clear()
        return pd.DataFrame()
    elif response.status_code != 200:
        print('Warning: Unexpected status code', response.status_code, 'for', url)

    try:
        return pd.read_parquet(io.BytesIO(response.content), engine='pyarrow')
    except pyarrow.lib.ArrowInvalid:
        print("No data available for =", url)
        return pd.DataFrame()
_download_cached = lakeapi._cache.cached(_download_one, ignore = ['auth', 'username'])

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
            "dt" in partition
            and (
                start is None
                or start.date() <= datetime.date.fromisoformat(partition["dt"])
            )
            and (
                end is None or end.date() > datetime.date.fromisoformat(partition["dt"])
            )
            and (symbols is None or partition["symbol"] in symbols)
            and (exchanges is None or partition["exchange"] in exchanges)
        )

    if table:
        path = f"s3://{bucket}/{table}/"
    else:
        path = f"s3://{bucket}/"
    path_root = lakeapi._read_parquet._get_path_root(path=path, dataset=True)
    paths = []

    if bucket == default_bucket and not last_modified_begin and not last_modified_end and table:
        paths = _get_table_contents_cache_key(boto3_session, bucket, table)

    if not paths:
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

    paths = lakeapi._read_parquet._apply_partition_filter(path_root=path_root, paths=paths, filter_func=partition_filter)
    if len(paths) < 1:
        raise lakeapi.exceptions.NoFilesFound(f"No files Found on: {path}.")
    return [_path_to_dict(path) for path in paths]

contents_cache = FSLRUCache(maxsize=32, path = '.lake_cache/contents', ttl=3600)
@cachetools.cached(cache=contents_cache, key=lambda sess, bucket, table: f'bucket={bucket[:7]}-table={table}')
def _get_table_contents_cache_key(boto3_session: boto3.Session, bucket: str, table: str):
    try:
        s3 = boto3_session.client('s3')
        s3_bucket, prefix = bucket.split('/', 1)
        key = f'{prefix}/{table}/contents.json.gz'
        obj = s3.get_object(Bucket=s3_bucket, Key=key)
        paths = json.loads(zlib.decompress(obj['Body'].read()).decode('utf-8'))['objects']
        paths = [f's3://{s3_bucket}/{path}' for path in paths]
        return paths
    except Exception as ex:
        print('Warning: error while fetching from contents cache at =', bucket, table, ', using slower method:', ex)

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

@cachetools.cached(cache=FSLRUCache(maxsize=8, path = '.lake_cache/used_data', ttl=60), key=lambda sess=None: f'data')
def used_data(boto3_session: Optional[boto3.Session] = None):
    '''
    Get used data in gigabytes. Note that the calculation can have to 60 minute delay!

    Example:
    {
    "downloaded_gb": 151.35,
    "timeframe_days": 31,
    "user": "my-user-email@gmail.com",
    "update_timestamp": 1656585600,
    }
    '''
    if boto3_session is None:
        boto3_session = boto3.Session(region_name="eu-west-1")
    user_arn, _method = _login(boto3_session, 'trades')
    username = user_arn.split('/')[-1]
    response = requests.get('https://api.crypto-lake.com/' + username, headers = {'User-Agent': f'lakeapi/{lakeapi.__version__}'})
    response.raise_for_status()
    return response.json()


if __name__ == "__main__":
    # session = boto3.Session(profile_name='', region_name="eu-west-1")
    # Test
    # df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 3), end = None, symbols = ['BTC-USDT'], exchanges = ['BINANCE']) # noqa
    # df = load_data(table = 'trades_mpid', start = datetime.datetime.now() - datetime.timedelta(days = 3), end = None, symbols = ['stSOL-USDC'], exchanges = ['SERUM']) # noqa
    # df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 2), end = None, symbols = None, exchanges = ['BINANCE']) # noqa
    # df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 4), end = None, symbols = ['TEST-USDT'], exchanges = None) #, boto3_session=session) # noqa
    df = load_data(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 465), end = datetime.datetime.now() - datetime.timedelta(days = 464), symbols = ['BTC-USDT-PERP'], exchanges = ['BINANCE_FUTURES']) #, boto3_session=session) # noqa
    # df = load_data(table = 'book', start = datetime.datetime.now() - datetime.timedelta(days = 1), end = None, symbols = ['BTC-USDT'], exchanges = ['BINANCE']) # noqa
    # df = _load_data_cloudfront(table = 'trades', start = datetime.datetime.now() - datetime.timedelta(days = 2), end = None, symbols = ['XCAD-USDT'], exchanges = None) # noqa
    # df = load_data(
    #     table="book",
    #     start=None, #datetime.datetime.now() - datetime.timedelta(days=2),
    #     end=None,
    #     symbols=["FTRB-USDT"],
    #     exchanges=None,
    # )
    pd.set_option("display.width", 1000)
    pd.set_option("display.max_columns", 30)
    print(df)
    # print(df.sample(20))
    print(df.dtypes)
    print(df.memory_usage().sum() / 1e6, "MB")
