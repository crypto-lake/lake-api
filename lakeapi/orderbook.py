# import numpy as np
from typing import TYPE_CHECKING, Optional, Tuple
from numba import float64, njit, jit
# from numba.core import types
from numba.typed import Dict

if TYPE_CHECKING:
	import pandas as pd


class OrderBookUpdater:
	''' Maintains order book snapshot while iterating over a dataframe with order book deltas. '''
	def __init__(self, df: 'pd.DataFrame'):
		self.bid = Dict.empty(key_type = float64, value_type = float64)
		self.ask = Dict.empty(key_type = float64, value_type = float64)
		self.current_index = 0
		self.received_timestamp = None
		self.np_arr = df[['bids', 'asks']].to_numpy()

	@staticmethod
	@njit(cache = False)
	def _update(bids, asks, bid_book, ask_book):
		if len(bids):
			for price, size in bids:
				if size == 0:
					if price in bid_book:
						del bid_book[price]
				else:
					bid_book[price] = size
		if len(asks) > 0:
			for price, size in asks:
				if size == 0:
					if price in ask_book:
						del ask_book[price]
				else:
					ask_book[price] = size

	def process_next_row(self, row: Optional[int] = None) -> None:
		''' row in df contains received_time, bid and ask columns with numpy list of price-quantity pairs'''
		if self.current_index >= self.np_arr.shape[0]:
			# return
			raise StopIteration
		if row is not None:
			self.current_index = row

		self._update(*self.np_arr[self.current_index], self.bid, self.ask)
		# self.received_timestamp = self.np_arr[self.current_index][0]
		self.current_index += 1

	def get_bests(self) -> Tuple[float, float]:
		# TODO speed up
		# return list(self.bid.keys())[-1], next(iter(self.ask.keys()))
		return max(self.bid), min(self.ask)
