=====
Usage
=====

If you don't have a paid plan with AWS credentials, you can access sample data::

    lakeapi.use_sample_data(anonymous_access = True)

    df = lakeapi.load_data(
        table="book",
        start=None,
        end=None,
        symbols=["BTC-USDT"],
        exchanges=None,
    )

With paid access, you can query any data::

    import lakeapi

    # Downloads BTC-USDT depth snapshots for last 2 days from all available exchanges
    df = lakeapi.load_data(
        table="trades",
        start=datetime.datetime.now() - datetime.timedelta(days=2),
        end=None,
        symbols=["SOL-USDT"],
        exchanges=["KUCOIN"],
    )
