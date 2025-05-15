from typing import TYPE_CHECKING, Optional, Tuple
from numba import float64, njit
from numba.typed import Dict, List

if TYPE_CHECKING:
	import pandas as pd


class OrderBookUpdater:
	''' Maintains order book snapshot while iterating over a dataframe with order book deltas. '''
	def __init__(self, df: 'pd.DataFrame', depth_limit_min: int = 3000, depth_limit_max: int = 5000, debug: bool = False):
		self.bid = Dict.empty(key_type = float64, value_type = float64)
		self.ask = Dict.empty(key_type = float64, value_type = float64)
		self.received_timestamp = None
		self.sequence_number = None
		self.depth_limit_min = depth_limit_min
		self.depth_limit_max = depth_limit_max
		self._bests_cache = List()
		self._bests_cache.append(float('inf'))
		self._bests_cache.append(0.)
		# self._lazy_cache = List()
		# self._lazy_cache.append(0.)
		# self._lazy_cache.append(0.)

		self._debug = debug
		self.switch_to_next_day(df)

	def switch_to_next_day(self, df: 'pd.DataFrame') -> None:
		self.current_index = 0
		self._received_time = df['received_time'].astype('int64').values
		self._sequence_number = df['sequence_number'].astype('int64').values
		self._side_is_bid = df['side_is_bid'].astype('float64').values
		self._price = df['price'].astype('float64').values
		self._size = df['size'].astype('float64').values

		if self.bid and self.ask:
			self._check_bests()

	def _check_bests(self) -> None:
		i = 0
		while True:
			bbid, bask = self.get_bests()
			if bbid >= bask:
				if i == 0:
					bid = sorted(self.bid.keys(), reverse=True)[:10]
					ask = sorted(self.ask.keys())[:10]
				# Some old price level was not removed from the book. We should remove it.
				# We drop best bid and ask as we don't know which one is the stale one.
				del self.bid[bbid]
				del self.ask[bask]
				self._bests_cache[0] = float('inf')
				self._bests_cache[1] = 0.
				i += 1
			else:
				if i != 0:
					print('Warning: Best prices crossed, dropped', i, 'levels to fix it. Bid:', bid, 'Ask:', ask)
				break

	@staticmethod
	@njit(cache = True)
	def _update_more(side_is_bid, prices, sizes, received_time, sequence_number, current_index, bid_book, ask_book, bests_cache, depth_limit_min, depth_limit_max):
		starting_received_time = received_time[current_index]
		while received_time[current_index] == starting_received_time:
			price = prices[current_index]
			size = sizes[current_index]
			if side_is_bid[current_index]:
				if size == 0:
					if price in bid_book:
						del bid_book[price]
					if bests_cache[0] == price:
						bests_cache[0] = float('inf')
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

			# cleanup
			if len(bid_book) > depth_limit_max:
				sorted_bids = sorted(bid_book.keys(), reverse=True)
				for price in sorted_bids[depth_limit_min:]:
					del bid_book[price]
			elif len(ask_book) > depth_limit_max:
				sorted_asks = sorted(ask_book.keys())
				for price in sorted_asks[depth_limit_min:]:
					del ask_book[price]

		return current_index, sequence_number[current_index-1], received_time[current_index-1]

	def process_next_update(self, starting_row: Optional[int] = None) -> int:
		''' row in df contains received_time, bid and ask columns with numpy list of price-quantity pairs'''
		if self.current_index >= self._side_is_bid.shape[0]:
			return 0
		if starting_row is not None:
			self.current_index = starting_row

		self.current_index, self.sequence_number, self.received_timestamp = \
			self._update_more(
				self._side_is_bid, self._price, self._size, self._received_time, self._sequence_number,
				self.current_index, self.bid, self.ask, self._bests_cache, self.depth_limit_min, self.depth_limit_max
			)

		return self.current_index

	def get_bests(self) -> Tuple[float, float]:
		if self._bests_cache[0] == float('inf'):
			self._bests_cache[0] = max(self.bid)
		if not self._bests_cache[1]:
			self._bests_cache[1] = min(self.ask)
		return tuple(self._bests_cache)
