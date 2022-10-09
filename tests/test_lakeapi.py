import datetime

import pytest # noqa

import lakeapi # noqa


@pytest.fixture
def candles():
    lakeapi.use_sample_data()
    return lakeapi.load_data(
        table = 'candles',
        symbols = ['BTC-USDT'],
        exchanges = ['BINANCE'],
        start = datetime.datetime(2022, 8, 28),
        end = datetime.datetime(2022, 8, 30),
    )

def test_load_data_loads_something(candles):
    assert candles.shape[0] == 2 * 24 * 60

def test_load_data_dtypes(candles):
    print(candles.dtypes)
    assert 'str' not in set(candles.dtypes)
    assert str(candles.symbol.dtype) == 'category'
