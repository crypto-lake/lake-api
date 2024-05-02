from typing import TYPE_CHECKING, Optional, Tuple
from numba import float64, njit
from numba.typed import Dict, List

if TYPE_CHECKING:
	import pandas as pd


class OrderBookUpdater:
	''' Maintains order book snapshot while iterating over a dataframe with order book deltas. '''
	def __init__(self, df: 'pd.DataFrame'):
		self.bid = Dict.empty(key_type = float64, value_type = float64)
		self.ask = Dict.empty(key_type = float64, value_type = float64)
		self.current_index = 0
		self.received_timestamp = None
		self.sequence_number = None
		self.int_arr = df[['received_time', 'sequence_number']].astype('int64').values
		self.np_arr = df[['side_is_bid', 'price', 'size']].astype('float64').values
		self._bests_cache = List()
		self._bests_cache.append(0.)
		self._bests_cache.append(0.)

	@staticmethod
	@njit(cache = True)
	def _update_more(side_is_bid, prices, sizes, received_time, sequence_number, current_index, bid_book, ask_book, bests_cache):
		starting_received_time = received_time[current_index]
		while received_time[current_index] == starting_received_time:
			price = prices[current_index]
			size = sizes[current_index]
			if side_is_bid[current_index]:
				if size == 0:
					if price in bid_book:
						del bid_book[price]
					if bests_cache[0] == price:
						bests_cache[0] = 0.
				else:
					bid_book[price] = size
					if price > bests_cache[0]:
						bests_cache[0] = price
			else:
				if size == 0:
					if price in ask_book:
						del ask_book[price]
					if bests_cache[1] == price:
						bests_cache[1] = 0.
				else:
					ask_book[price] = size
					if price < bests_cache[1]:
						bests_cache[1] = price
			current_index += 1
			if current_index >= prices.shape[0]:
				break
		return current_index, sequence_number[current_index-1], received_time[current_index-1]

	def process_next_update(self, starting_row: Optional[int] = None) -> int:
		''' row in df contains received_time, bid and ask columns with numpy list of price-quantity pairs'''
		if self.current_index >= self.np_arr.shape[0]:
			return 0
		if starting_row is not None:
			self.current_index = starting_row

		self.current_index, self.sequence_number, self.received_timestamp = \
			self._update_more(*self.np_arr.T, *self.int_arr.T, self.current_index, self.bid, self.ask, self._bests_cache)

		return self.current_index

	def get_bests(self) -> Tuple[float, float]:
		if not self._bests_cache[0]:
			self._bests_cache[0] = max(self.bid)
		if not self._bests_cache[1]:
			self._bests_cache[1] = min(self.ask)
		return tuple(self._bests_cache)
