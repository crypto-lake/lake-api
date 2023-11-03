"""Amazon S3 Describe Module (INTERNAL)."""

import concurrent.futures
import datetime
import itertools
import logging
from typing import Any, Dict, List, Optional, Tuple, Union, Sequence, Iterator

import boto3

from lakeapi import _utils, _fs, exceptions

_logger: logging.Logger = logging.getLogger(__name__)


def _path2list(
    path: Union[str, Sequence[str]],
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    suffix: Union[str, List[str], None] = None,
    ignore_suffix: Union[str, List[str], None] = None,
    ignore_empty: bool = False,
) -> List[str]:
    """Convert Amazon S3 path to list of objects."""
    _suffix: Optional[List[str]] = [suffix] if isinstance(suffix, str) else suffix
    _ignore_suffix: Optional[List[str]] = [ignore_suffix] if isinstance(ignore_suffix, str) else ignore_suffix
    if isinstance(path, str):  # prefix
        paths: List[str] = list_objects(  # type: ignore
            path=path,
            suffix=_suffix,
            ignore_suffix=_ignore_suffix,
            boto3_session=boto3_session,
            last_modified_begin=last_modified_begin,
            last_modified_end=last_modified_end,
            ignore_empty=ignore_empty,
            s3_additional_kwargs=s3_additional_kwargs,
        )
    elif isinstance(path, list):
        if last_modified_begin or last_modified_end:
            raise exceptions.InvalidArgumentCombination(
                "Specify a list of files or (last_modified_begin and last_modified_end)"
            )
        paths = path if _suffix is None else [x for x in path if x.endswith(tuple(_suffix))]
        paths = path if _ignore_suffix is None else [x for x in paths if x.endswith(tuple(_ignore_suffix)) is False]
    else:
        raise exceptions.InvalidArgumentType(f"{type(path)} is not a valid path type. Please, use str or List[str].")
    return paths


def list_objects(
    path: str,
    suffix: Union[str, List[str], None] = None,
    ignore_suffix: Union[str, List[str], None] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    ignore_empty: bool = False,
    chunked: bool = False,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Union[List[str], Iterator[List[str]]]:
    """List Amazon S3 objects from a prefix.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    The filter by last_modified begin last_modified end is applied after list all S3 files

    Parameters
    ----------
    path : str
        S3 path (e.g. s3://bucket/prefix).
    suffix: Union[str, List[str], None]
        Suffix or List of suffixes for filtering S3 keys.
    ignore_suffix: Union[str, List[str], None]
        Suffix or List of suffixes for S3 keys to be ignored.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    ignore_empty: bool
        Ignore files with 0 bytes.
    chunked: bool
        If True returns iterator, and a single list otherwise. False by default.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Union[List[str], Iterator[List[str]]]
        List of objects paths.

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix')
    ['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> wr.s3.list_objects('s3://bucket/prefix', boto3_session=boto3.Session())
    ['s3://bucket/prefix0', 's3://bucket/prefix1', 's3://bucket/prefix2']

    """
    # On top of user provided ignore_suffix input, add "/"
    ignore_suffix_acc = set("/")
    if isinstance(ignore_suffix, str):
        ignore_suffix_acc.add(ignore_suffix)
    elif isinstance(ignore_suffix, list):
        ignore_suffix_acc.update(ignore_suffix)

    result_iterator = _list_objects(
        path=path,
        delimiter=None,
        suffix=suffix,
        ignore_suffix=list(ignore_suffix_acc),
        boto3_session=boto3_session,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        ignore_empty=ignore_empty,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if chunked:
        return result_iterator
    return [path for paths in result_iterator for path in paths]

def _list_objects(  # pylint: disable=too-many-branches
    path: str,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    delimiter: Optional[str] = None,
    suffix: Union[str, List[str], None] = None,
    ignore_suffix: Union[str, List[str], None] = None,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    boto3_session: Optional[boto3.Session] = None,
    ignore_empty: bool = False,
) -> Iterator[List[str]]:
    bucket: str
    prefix_original: str
    bucket, prefix_original = _utils.parse_path(path=path)
    prefix: str = _prefix_cleanup(prefix=prefix_original)
    _suffix: Union[List[str], None] = [suffix] if isinstance(suffix, str) else suffix
    _ignore_suffix: Union[List[str], None] = [ignore_suffix] if isinstance(ignore_suffix, str) else ignore_suffix
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    default_pagination: Dict[str, int] = {"PageSize": 1000}
    extra_kwargs: Dict[str, Any] = {"PaginationConfig": default_pagination}
    if s3_additional_kwargs:
        extra_kwargs = _fs.get_botocore_valid_kwargs(
            function_name="list_objects_v2", s3_additional_kwargs=s3_additional_kwargs
        )
        extra_kwargs["PaginationConfig"] = (
            s3_additional_kwargs["PaginationConfig"]
            if "PaginationConfig" in s3_additional_kwargs
            else default_pagination
        )
    paginator = client_s3.get_paginator("list_objects_v2")
    args: Dict[str, Any] = {"Bucket": bucket, "Prefix": prefix, **extra_kwargs}
    if delimiter is not None:
        args["Delimiter"] = delimiter
    _logger.debug("args: %s", args)
    response_iterator = paginator.paginate(**args)
    paths: List[str] = []
    _validate_datetimes(last_modified_begin=last_modified_begin, last_modified_end=last_modified_end)

    for page in response_iterator:  # pylint: disable=too-many-nested-blocks
        if delimiter is None:
            contents: Optional[List[Dict[str, Any]]] = page.get("Contents")
            if contents is not None:
                for content in contents:
                    key: str = content["Key"]
                    if ignore_empty and content.get("Size", 0) == 0:
                        _logger.debug("Skipping empty file: %s", f"s3://{bucket}/{key}")
                    elif (content is not None) and ("Key" in content):
                        if (_suffix is None) or key.endswith(tuple(_suffix)):
                            if last_modified_begin is not None:
                                if content["LastModified"] < last_modified_begin:
                                    continue
                            if last_modified_end is not None:
                                if content["LastModified"] > last_modified_end:
                                    continue
                            paths.append(f"s3://{bucket}/{key}")
        else:
            prefixes: Optional[List[Optional[Dict[str, str]]]] = page.get("CommonPrefixes")
            if prefixes is not None:
                for pfx in prefixes:
                    if (pfx is not None) and ("Prefix" in pfx):
                        key = pfx["Prefix"]
                        paths.append(f"s3://{bucket}/{key}")

        if prefix != prefix_original:
            raise ValueError('Wildcard in prefix')
            # paths = fnmatch.filter(paths, path)

        if _ignore_suffix is not None:
            paths = [p for p in paths if p.endswith(tuple(_ignore_suffix)) is False]

        if paths:
            yield paths
        paths = []


def _validate_datetimes(
    last_modified_begin: Optional[datetime.datetime] = None, last_modified_end: Optional[datetime.datetime] = None
) -> None:
    if (last_modified_begin is not None) and (last_modified_begin.tzinfo is None):
        raise exceptions.InvalidArgumentValue("Timezone is not defined for last_modified_begin.")
    if (last_modified_end is not None) and (last_modified_end.tzinfo is None):
        raise exceptions.InvalidArgumentValue("Timezone is not defined for last_modified_end.")
    if (last_modified_begin is not None) and (last_modified_end is not None):
        if last_modified_begin > last_modified_end:
            raise exceptions.InvalidArgumentValue("last_modified_begin is bigger than last_modified_end.")

def _prefix_cleanup(prefix: str) -> str:
    for n, c in enumerate(prefix):
        if c in ["*", "?", "["]:
            return prefix[:n]
    return prefix

def _get_path_root(path: Union[str, List[str]], dataset: bool) -> Optional[str]:
    if (dataset is True) and (not isinstance(path, str)):
        raise exceptions.InvalidArgument("The path argument must be a string if dataset=True (Amazon S3 prefix).")
    return _prefix_cleanup(str(path)) if dataset is True else None


def _describe_object(
    path: str,
    boto3_session: boto3.Session,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    version_id: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    bucket: str
    key: str
    bucket, key = _utils.parse_path(path=path)
    if s3_additional_kwargs:
        extra_kwargs: Dict[str, Any] = _fs.get_botocore_valid_kwargs(
            function_name="head_object", s3_additional_kwargs=s3_additional_kwargs
        )
    else:
        extra_kwargs = {}
    desc: Dict[str, Any]
    if version_id:
        extra_kwargs["VersionId"] = version_id
    desc = _utils.try_it(
        f=client_s3.head_object, ex=client_s3.exceptions.NoSuchKey, Bucket=bucket, Key=key, **extra_kwargs
    )
    return path, desc


def _describe_object_concurrent(
    path: str,
    boto3_primitives: _utils.Boto3PrimitivesType,
    s3_additional_kwargs: Optional[Dict[str, Any]],
    version_id: Optional[str] = None,
) -> Tuple[str, Dict[str, Any]]:
    boto3_session = _utils.boto3_from_primitives(primitives=boto3_primitives)
    return _describe_object(
        path=path, boto3_session=boto3_session, s3_additional_kwargs=s3_additional_kwargs, version_id=version_id
    )


def describe_objects(
    path: Union[str, List[str]],
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    use_threads: Union[bool, int] = True,
    last_modified_begin: Optional[datetime.datetime] = None,
    last_modified_end: Optional[datetime.datetime] = None,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Dict[str, Any]]:
    """Describe Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    Fetch attributes like ContentLength, DeleteMarker, last_modified, ContentType, etc
    The full list of attributes can be explored under the boto3 head_object documentation:
    https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Note
    ----
    The filter by last_modified begin last_modified end is applied after list all S3 files

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    last_modified_begin
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    last_modified_end: datetime, optional
        Filter the s3 files by the Last modified date of the object.
        The filter is applied only after list all s3 files.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Dict[str, Any]]
        Return a dictionary of objects returned from head_objects where the key is the object path.
        The response object can be explored here:
        https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3.html#S3.Client.head_object

    Examples
    --------
    >>> import awswrangler as wr
    >>> descs0 = wr.s3.describe_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Describe both objects
    >>> descs1 = wr.s3.describe_objects('s3://bucket/prefix')  # Describe all objects under the prefix

    """
    paths: List[str] = _path2list(
        path=path,
        boto3_session=boto3_session,
        last_modified_begin=last_modified_begin,
        last_modified_end=last_modified_end,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    if len(paths) < 1:
        return {}
    resp_list: List[Tuple[str, Dict[str, Any]]]
    if len(paths) == 1:
        resp_list = [
            _describe_object(
                path=paths[0],
                version_id=version_id.get(paths[0]) if isinstance(version_id, dict) else version_id,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
        ]
    elif use_threads is False:
        resp_list = [
            _describe_object(
                path=p,
                version_id=version_id.get(p) if isinstance(version_id, dict) else version_id,
                boto3_session=boto3_session,
                s3_additional_kwargs=s3_additional_kwargs,
            )
            for p in paths
        ]
    else:
        cpus: int = _utils.ensure_cpu_count(use_threads=use_threads)
        versions = [version_id.get(p) if isinstance(version_id, dict) else version_id for p in paths]
        with concurrent.futures.ThreadPoolExecutor(max_workers=cpus) as executor:
            resp_list = list(
                executor.map(
                    _describe_object_concurrent,
                    paths,
                    versions,
                    itertools.repeat(_utils.boto3_to_primitives(boto3_session=boto3_session)),
                    itertools.repeat(s3_additional_kwargs),
                )
            )
    desc_dict: Dict[str, Dict[str, Any]] = dict(resp_list)
    return desc_dict


def size_objects(
    path: Union[str, List[str]],
    version_id: Optional[Union[str, Dict[str, str]]] = None,
    use_threads: Union[bool, int] = True,
    s3_additional_kwargs: Optional[Dict[str, Any]] = None,
    boto3_session: Optional[boto3.Session] = None,
) -> Dict[str, Optional[int]]:
    """Get the size (ContentLength) in bytes of Amazon S3 objects from a received S3 prefix or list of S3 objects paths.

    This function accepts Unix shell-style wildcards in the path argument.
    * (matches everything), ? (matches any single character),
    [seq] (matches any character in seq), [!seq] (matches any character not in seq).
    If you want to use a path which includes Unix shell-style wildcard characters (`*, ?, []`),
    you can use `glob.escape(path)` before passing the path to this function.

    Note
    ----
    In case of `use_threads=True` the number of threads
    that will be spawned will be gotten from os.cpu_count().

    Parameters
    ----------
    path : Union[str, List[str]]
        S3 prefix (accepts Unix shell-style wildcards)
        (e.g. s3://bucket/prefix) or list of S3 objects paths (e.g. [s3://bucket/key0, s3://bucket/key1]).
    version_id: Optional[Union[str, Dict[str, str]]]
        Version id of the object or mapping of object path to version id.
        (e.g. {'s3://bucket/key0': '121212', 's3://bucket/key1': '343434'})
    use_threads : bool, int
        True to enable concurrent requests, False to disable multiple threads.
        If enabled os.cpu_count() will be used as the max number of threads.
        If integer is provided, specified number is used.
    s3_additional_kwargs : Optional[Dict[str, Any]]
        Forwarded to botocore requests.
        e.g. s3_additional_kwargs={'RequestPayer': 'requester'}
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    Dict[str, Optional[int]]
        Dictionary where the key is the object path and the value is the object size.

    Examples
    --------
    >>> import awswrangler as wr
    >>> sizes0 = wr.s3.size_objects(['s3://bucket/key0', 's3://bucket/key1'])  # Get the sizes of both objects
    >>> sizes1 = wr.s3.size_objects('s3://bucket/prefix')  # Get the sizes of all objects under the received prefix

    """
    desc_list: Dict[str, Dict[str, Any]] = describe_objects(
        path=path,
        version_id=version_id,
        use_threads=use_threads,
        boto3_session=boto3_session,
        s3_additional_kwargs=s3_additional_kwargs,
    )
    size_dict: Dict[str, Optional[int]] = {k: d.get("ContentLength", None) for k, d in desc_list.items()}
    return size_dict


def get_bucket_region(bucket: str, boto3_session: Optional[boto3.Session] = None) -> str:
    """Get bucket region name.

    Parameters
    ----------
    bucket : str
        Bucket name.
    boto3_session : boto3.Session(), optional
        Boto3 Session. The default boto3 session will be used if boto3_session receive None.

    Returns
    -------
    str
        Region code (e.g. 'us-east-1').

    Examples
    --------
    Using the default boto3 session

    >>> import awswrangler as wr
    >>> region = wr.s3.get_bucket_region('bucket-name')

    Using a custom boto3 session

    >>> import boto3
    >>> import awswrangler as wr
    >>> region = wr.s3.get_bucket_region('bucket-name', boto3_session=boto3.Session())

    """
    client_s3: boto3.client = _utils.client(service_name="s3", session=boto3_session)
    _logger.debug("bucket: %s", bucket)
    region: str = client_s3.get_bucket_location(Bucket=bucket)["LocationConstraint"]
    region = "us-east-1" if region is None else region
    _logger.debug("region: %s", region)
    return region
