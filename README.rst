========
Lake API
========


.. image:: https://img.shields.io/pypi/v/lakeapi.svg
        :target: https://pypi.python.org/pypi/lakeapi
        :alt: Pypi package status

.. image:: https://readthedocs.org/projects/lake-api/badge/?version=latest
        :target: https://lake-api.readthedocs.io/en/latest/?version=latest
        :alt: Documentation status

.. image:: https://github.com/crypto-lake/lake-api/actions/workflows/dev.yml/badge.svg
     :target: https://github.com/crypto-lake/lake-api/actions/workflows/dev.yml
     :alt: Build status


API for accessing Lake crypto market data.

Lake is a service providing `historical cryptocurrency market data <https://crypto-lake.com/>`_ in high detail, including `order book data <https://crypto-lake.com/order-book-data/>`_, tick trades and 1m trade candles. It is tuned for convenient quant and machine-learning purposes and so offers high performance, caching and parallelization.


* Web: https://crypto-lake.com/
* Documentation: https://lake-api.readthedocs.io.
* Online example -- executable collab notebook: https://colab.research.google.com/drive/1E7MSUT8xqYTMVLiq_rMBLNcZmI_KusK3


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
        exchanges=["BINANCE"],
    )


With paid access, you can query any data:

.. code-block:: python

    import lakeapi

    # Downloads SOL-USDT depth snapshots for last 2 days from Kucoin exchange
    df = lakeapi.load_data(
        table="trades",
        start=datetime.datetime.now() - datetime.timedelta(days=2),
        end=None,
        symbols=["SOL-USDT"],
        exchanges=["KUCOIN"],
    )

We recommend putting .lake_cache directory into .gitignore, because Lake API stores cache into this directory in the
working directory.
