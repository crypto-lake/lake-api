'''
Cache downloaded data

You can adjust module-level `bytes_limit`, but only before you use the cache.
'''
from typing import Any, Callable
import joblib

default_bytes_limit: int = 10_000_000_000
verbose_cache = 0

_store: joblib.Memory = joblib.Memory(
	'.lake_cache',
	compress = 0,
	bytes_limit = default_bytes_limit,
	verbose = verbose_cache,
)
cached: Callable[..., Callable[..., Any]] = _store.cache

if __name__ == '__main__':
	import time

	@cached
	def double(x):
		time.sleep(3)
		return 2*x
