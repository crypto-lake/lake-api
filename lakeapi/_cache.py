'''
Cache downloaded data
'''
from typing import Any, Callable
import joblib

bytes_limit: int = 1_000_000_000_000
verbose_cache = 0

_store: joblib.Memory = joblib.Memory(
	'.lake_cache',
	compress = False,
	verbose = verbose_cache,
)
cached: Callable[..., Callable[..., Any]] = _store.cache

if __name__ == '__main__':
	import time

	@cached
	def double(x):
		time.sleep(3)
		return 2*x
