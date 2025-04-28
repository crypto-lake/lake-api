=======
History
=======

0.20.1 (2025-04-28)
-------------------

* fix UnboundLocalError in _get_table_contents_cache_key when non-existing table is passed

0.20.0 (2025-03-28)
-------------------

* new used_data function
* clear cache on 404 to read new contents

0.19.1 (2024-12-16)
-------------------

* fix typing issues

0.19.0 (2024-09-09)
-------------------

* fix possible KeyError: 'dt' with list_data

0.18.0 (2024-08-21)
-------------------

* fix KeyError: 'dt' with s3 method

0.17.0 (2024-08-09)
-------------------

* orderbook class performance improvements

0.16.0 (2024-08-06)
-------------------

* improve data loading performance, especially for the first time

0.15.0 (2024-07-24)
-------------------

* fix conflict of new numpy 2 and older pyarrow
* improve tox tests
* add missing numba dependency needed by orderbook class (the rest works without numba)

0.14.0 (2023-05-02)
-------------------

* order book helper class

0.13.0 (2023-03-15)
-------------------

* fix old funding rates

0.12.1 (2023-11-22)
-------------------

* fix issue !6 with lowercase symbol names
* better error when aws clients use free data sample (!7)

0.12.0 (2023-11-18)
-------------------

* option to disable cache
* improved warnings handling

0.11.2 (2023-11-09)
-------------------

* fix order book data loading (KeyError: side)

0.11.1 (2023-11-06)
-------------------

* minor optimizations and fixes in cloudfront transfer

0.10.0 (2023-11-06)
-------------------

* more efficient optional data transfer implemented via aws cloudfront

0.9.1 (2023-11-03)
------------------

* awswrangler dependency removed, pandas 2 support added
* python3.12 support

0.8.0 (2023-09-18)
------------------

* grow default cache size limit
* nicer error messages when data are missing
* pass and print warning when file is corrupted

0.7.0 (2023-09-18)
------------------

* let user specify max cache size via `lakeapi.set_cache_size_limit()`

0.6.4 (2023-08-05)
------------------

* too many open files bugfix

0.6.3 (2023-08-03)
------------------

* logging fixes

0.6.2 (2023-08-18)
------------------

* fix dependency constraints causing `TypeError: _path2list() got an unexpected keyword argument 'boto3_session'`

0.6.1 (2023-08-15)
------------------

* fix path2list bug
* fix type hints

0.6.0 (2023-08-14)
------------------

* support for python3.11

0.5.0 (2023-05-21)
------------------

* support for funding, open_interest and liquidations list_data
* improve data type typing

0.4.5 (2023-01-09)
------------------

* grow default cache size limit from 3 GB to 10 GB

0.4.3 (2022-12-09)
------------------

* small documentation improvements

0.4.2 (2022-12-09)
------------------

* fix trades_mpid issue

0.4.1 (2022-12-05)
------------------

* fix warning messages in anonymous mode

0.4.0 (2022-11-19)
------------------

* level_1 data added to typing
* s3 user agent set to lakeapi

0.3.0 (2022-11-04)
------------------

* Typing bugfix
* Last modified filters for list_data

0.2.0 (2022-10-26)
------------------

* New feature for listing available data.

0.1.3 (2022-10-13)
------------------

* Corrupted cache bugfix

0.1.2 (2022-10-10)
------------------

* Caching and requirements improvements.

0.1.1 (2022-10-09)
------------------

* Python2.7 support and documentation improvements.

0.1.0 (2022-10-08)
------------------

* First release on PyPI.
