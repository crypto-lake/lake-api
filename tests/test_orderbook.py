import pandas as pd
import numpy as np
import pytest

from lakeapi.orderbook import OrderBookUpdater

@pytest.fixture
def order_book_updater(example_data):
	df = pd.DataFrame(example_data)
	df['side_is_bid'] = df['side_is_bid'].apply(lambda x: np.array(x))
	df['price'] = df['price'].apply(lambda x: np.array(x))
	df['size'] = df['size'].apply(lambda x: np.array(x))
	df['received_time'] = df['received_time'].apply(lambda x: np.array(x))
	return OrderBookUpdater(df)

@pytest.fixture
def example_data():
	return [
		{
			'received_time': 1,
			'sequence_number': 1,
			'side_is_bid': True,
			'price': 1,
			'size': 10,
		},
		{
			'received_time': 1,
			'sequence_number': 1,
			'side_is_bid': True,
			'price': 2,
			'size': 20,
		},
		{
			'received_time': 1,
			'sequence_number': 1,
			'side_is_bid': False,
			'price': 3,
			'size': 10,
		},
		{
			'received_time': 1,
			'sequence_number': 1,
			'side_is_bid': False,
			'price': 4,
			'size': 20,
		},
		{
			'received_time': 2,
			'sequence_number': 2,
			'side_is_bid': True,
			'price': 1,
			'size': 5,
		},
		{
			'received_time': 2,
			'sequence_number': 2,
			'side_is_bid': False,
			'price': 3,
			'size': 5,
		},
		{
			'received_time': 3,
			'sequence_number': 3,
			'side_is_bid': True,
			'price': 2,
			'size': 0,
		},
		{
			'received_time': 3,
			'sequence_number': 3,
			'side_is_bid': False,
			'price': 4,
			'size': 0,
		},
	]

def test_process_next_update(order_book_updater, example_data):
	order_book_updater.process_next_update()
	assert order_book_updater.bid == {1: 10, 2:20}
	assert order_book_updater.ask == {3: 10, 4:20}
	assert order_book_updater.received_timestamp == example_data[0]['received_time']
	assert order_book_updater.sequence_number == example_data[0]['sequence_number']

	order_book_updater.process_next_update()
	assert order_book_updater.bid[1] == 5
	assert order_book_updater.bid[2] == 20

	order_book_updater.process_next_update()
	assert order_book_updater.ask == {3: 5}
	assert order_book_updater.received_timestamp == example_data[-1]['received_time']
	assert order_book_updater.sequence_number == example_data[-1]['sequence_number']


def test_get_bests(order_book_updater):
	order_book_updater.process_next_update()
	assert order_book_updater.get_bests() == (2, 3)

@pytest.mark.benchmark(group='process_next_update')
def test_process_next_update_benchmark(order_book_updater, benchmark):
	benchmark.pedantic(order_book_updater.process_next_update, args = (0,), warmup_rounds=100, iterations=1000, rounds=10)
