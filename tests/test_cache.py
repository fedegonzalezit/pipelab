import unittest
import tempfile
import shutil
from pipelab.cache import CachedPipelineMixin, InDiskCacheWrapper, InMemoryCacheWrapper
from pipelab.pipeline import PipelineStep


class DummyCache(CachedPipelineMixin):
    def __init__(self):
        self._cache = {}

    def set(self, key, value):
        self._cache[key] = value

    def get(self, key, default=None):
        return self._cache.get(key, default)

    def clear(self):
        self._cache.clear()


class DummyStepForCache(PipelineStep):
    def __init__(self, name=None, value=1):
        super().__init__(name)
        self.value = value

    def execute(self, x=0):
        return self.value + x

    def get_execute_params(self):
        return {}


class CachedPipelineMixinTests(unittest.TestCase):
    def setUp(self):
        self.cache = DummyCache()

    def test_set_and_get(self):
        self.cache.set("foo", 123)
        self.assertEqual(self.cache.get("foo"), 123)

    def test_get_default(self):
        self.assertIsNone(self.cache.get("bar"))
        self.assertEqual(self.cache.get("bar", 42), 42)

    def test_clear(self):
        self.cache.set("foo", 1)
        self.cache.clear()
        self.assertIsNone(self.cache.get("foo"))


class CacheWrappersTests(unittest.TestCase):
    def test_in_disk_cache_wrapper(self):
        tmpdir = tempfile.mkdtemp()
        step = DummyStepForCache("step1", value=5)
        wrapper = InDiskCacheWrapper(step, cache_dir=tmpdir)
        # First call: cache miss
        result1 = wrapper.execute(2)
        assert result1 == 7
        # Second call: cache hit
        result2 = wrapper.execute(2)
        assert result2 == 7
        # Test get_execute_params and name
        assert wrapper.get_execute_params() == {}
        assert wrapper.name == "step1"
        shutil.rmtree(tmpdir)

    def test_in_memory_cache_wrapper(self):
        step = DummyStepForCache("step2", value=10)
        wrapper = InMemoryCacheWrapper(step)
        # First call: cache miss
        result1 = wrapper.execute(3)
        assert result1 == 13
        # Second call: cache hit
        result2 = wrapper.execute(3)
        assert result2 == 13
        # Test get_execute_params and name
        assert wrapper.get_execute_params() == {}
        assert wrapper.name == "step2"

    def test_in_disk_cache_wrapper_serialize_error(self):
        tmpdir = tempfile.mkdtemp()
        step = DummyStepForCache("step3")
        wrapper = InDiskCacheWrapper(step, cache_dir=tmpdir)
        # Patch cloudpickle to raise error
        import cloudpickle

        orig_dumps = cloudpickle.dumps
        cloudpickle.dumps = lambda *a, **kw: (_ for _ in ()).throw(Exception("fail"))
        try:
            try:
                wrapper.execute(1)
            except ValueError as e:
                assert "Failed to serialize for cache" in str(e)
        finally:
            cloudpickle.dumps = orig_dumps
            shutil.rmtree(tmpdir)

    def test_cached_pipeline_mixin_methods(self):
        class DummyStep(PipelineStep):
            def execute(self):
                return 1

            def get_execute_params(self):
                return {}

        step = DummyStep()
        disk = step.in_disk_cache()
        mem = step.in_memory_cache()
        assert isinstance(disk, InDiskCacheWrapper)
        assert isinstance(mem, InMemoryCacheWrapper)


if __name__ == "__main__":
    unittest.main()
