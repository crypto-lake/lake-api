=======
History
=======

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
