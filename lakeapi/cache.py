'''
Cache downloaded data

You can adjust module-level `bytes_limit`, but only before you use the cache.
'''
from typing import Any, Callable
import joblib
from pathlib import Path

bytes_limit: int = 3_000_000_000
verbose_cache = 0

_store: joblib.Memory = joblib.Memory(
	Path(__file__).parent.parent / 'cache',
	compress = 2,
	bytes_limit = bytes_limit,
	verbose = verbose_cache,
)
cached: Callable[..., Callable[..., Any]] = _store.cache
_store.reduce_size()

if __name__ == '__main__':
	import time

	@cached
	def double(x):
		time.sleep(3)
		return 2*x
