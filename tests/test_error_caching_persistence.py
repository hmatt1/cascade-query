import pytest
import tempfile
from cascade.engine import Engine

def test_persistent_error_caching():
    with tempfile.TemporaryDirectory() as tmpdir:
        engine1 = Engine(cache_dir=tmpdir)
        
        calls1 = 0
        
        @engine1.query
        def throws():
            nonlocal calls1
            calls1 += 1
            raise ValueError("Disk cached error")
            
        with pytest.raises(ValueError, match="Disk cached error"):
            throws()
        assert calls1 == 1
        
        engine1.shutdown()
        
        # Hydrate in new engine
        engine2 = Engine(cache_dir=tmpdir)
        calls2 = 0
        
        @engine2.query
        def throws():
            nonlocal calls2
            calls2 += 1
            raise ValueError("Disk cached error")
            
        with pytest.raises(ValueError, match="Disk cached error"):
            throws()
        
        # Should be hydrated from disk, so calls2 remains 0
        assert calls2 == 0
        
        engine2.shutdown()
