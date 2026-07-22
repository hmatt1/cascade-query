import os
import pytest
from cascade.engine import Engine

original_init = Engine.__init__

def patched_init(self, *args, **kwargs):
    if kwargs.get("cache_dir") is not None and os.environ.get("PYTHON_GIL") == "0":
        pytest.skip("lmdb is not thread-safe without GIL")
    original_init(self, *args, **kwargs)

Engine.__init__ = patched_init
