========
Lake API
========


.. image:: https://img.shields.io/pypi/v/lakeapi.svg
        :target: https://pypi.python.org/pypi/lakeapi
        :alt: Pypi package status

.. image:: https://readthedocs.org/projects/lake-api/badge/?version=latest
        :target: https://lakeapi.readthedocs.io/en/latest/?version=latest
        :alt: Documentation status

.. image:: https://github.com/crypto-lake/lake-api/actions/workflows/dev.yml/badge.svg
     :target: https://github.com/crypto-lake/lake-api/actions/workflows/dev.yml
     :alt: Build status



API for accessing Lake crypto market data.


* Free software: Apache 2.0 license
* Documentation: https://lakeapi.readthedocs.io.


Usage
-----

If you don't have a paid plan with AWS credentials, you can access sample data:

.. code-block:: python

    import lakeapi

    lakeapi.use_sample_data(anonymous_access = True)

    df = lakeapi.load_data(
        table="book",
        start=None,
        end=None,
        symbols=["BTC-USDT"],
        exchanges=None,
    )


With paid access, you can query any data:

.. code-block:: python

    import lakeapi

    # Downloads BTC-USDT depth snapshots for last 2 days from all available exchanges
    df = lakeapi.load_data(
        table="trades",
        start=datetime.datetime.now() - datetime.timedelta(days=2),
        end=None,
        symbols=["SOL-USDT"],
        exchanges=["KUCOIN"],
    )

