import pandas as pd
import numpy as np
import pytest

from lakeapi.orderbook import OrderBookUpdater

@pytest.fixture
def order_book_updater(example_data):
	df = pd.DataFrame(example_data)
	df['bids'] = df['bids'].apply(lambda x: np.array(x))
	df['asks'] = df['asks'].apply(lambda x: np.array(x))
	return OrderBookUpdater(df)

@pytest.fixture
def example_data():
	return [
		{
			'received_time': 1,
			'bids': [(1, 10), (2, 20)],
			'asks': [(3, 10), (4, 20)]
		},
		{
			'received_time': 2,
			'bids': [(1, 5)],
			'asks': [(3, 5)]
		},
		{
			'received_time': 3,
			'bids': [(2, 0)],
			'asks': [(4, 0)]
		}
	]

def test_process_next_row(order_book_updater, example_data):
	order_book_updater.process_next_row()
	assert order_book_updater.bid == dict(example_data[0]['bids'])
	assert order_book_updater.ask == dict(example_data[0]['asks'])
	assert order_book_updater.received_timestamp == example_data[0]['received_time']

	order_book_updater.process_next_row()
	assert order_book_updater.bid[1] == 5
	assert order_book_updater.bid[2] == 20

	order_book_updater.process_next_row()
	assert order_book_updater.ask == {3: 5}
	assert order_book_updater.received_timestamp == example_data[-1]['received_time']


def test_get_bests(order_book_updater):
	order_book_updater.process_next_row()
	assert order_book_updater.get_bests() == (2, 3)

@pytest.mark.benchmark(group='process_next_row')
def test_process_next_row_benchmark(order_book_updater, benchmark):
	benchmark.pedantic(order_book_updater.process_next_row, args = (0,), warmup_rounds=100, iterations=1000, rounds=10)
