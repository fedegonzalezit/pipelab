import os
import hashlib
import pickle
import inspect
from typing import Dict, Any, Optional, Self
import cloudpickle


class InDiskCacheWrapper:
    """
    Wrapper class to enable in-disk caching for pipeline steps.
    It uses the InDiskCache class to cache artifacts on disk.
    """

    def __init__(
        self,
        step,
        cache_dir: str = ".cache",
        execute_params: Optional[Dict[str, Any]] = None,
    ):
        self.step = step
        self.cache_dir = os.path.join(cache_dir, step.name)
        if not os.path.exists(self.cache_dir):
            os.makedirs(self.cache_dir)
        self._execute_params = execute_params or {}

    def execute(self, *args: Any, **kwargs: Any) -> None:
        """
        if the step has a cache, it hashes the parameters and checks if theresult is already cached.
        note that params could be any object, so it uses cloudpickle to serialize them.
        If the result is cached, it returns the cached result.
        If not, it executes the step and saves the result in the cache.
        """
        # Bind args/kwargs to parameter names using original signature
        bound = inspect.signature(self.step.execute).bind(*args, **kwargs)
        # bound.apply_defaults()

        # also checks que values from __init__ for the hash
        init_params = self.step.__dict__.copy()
        # si los parametros con los que se inicializo cambiaron entonces deberia missear el cache
        bound.apply_defaults()

        # Serialize input arguments with cloudpickle
        try:
            serialized = cloudpickle.dumps(bound.arguments)
            # Include init parameters in the serialization
            serialized += cloudpickle.dumps(init_params)
        except Exception as e:
            raise ValueError(f"Failed to serialize for cache: {e}")

        # Generate a hash key from inputs
        hash_key = hashlib.sha256(serialized).hexdigest()
        cache_file = os.path.join(self.cache_dir, f"{hash_key}.pkl")

        # Load from cache or compute and save
        if os.path.exists(cache_file):
            print(f"Loading cached result for {self.step.name} from {cache_file}")
            with open(cache_file, "rb") as f:
                return pickle.load(f)
        else:
            print(
                f"Cache miss for {self.step.name}, executing step and saving result to {cache_file}"
            )
            result = self.step.execute(*args, **kwargs)
            with open(cache_file, "wb") as f:
                pickle.dump(result, f)
            return result

    def get_execute_params(self) -> Dict[str, Any]:
        """
        Get the parameters for the execute method of the wrapped step.
        """
        return self._execute_params

    @property
    def name(self) -> str:
        """
        Get the name of the step.
        """
        return self.step.name


class InMemoryCacheWrapper:
    """
    Wrapper class to enable in-memory caching for pipeline steps.
    It uses the InMemoryCache class to cache artifacts in memory.
    """

    cache = {}

    def __init__(self, step, execute_params: Optional[Dict[str, Any]] = None):
        self.step = step
        self._execute_params = execute_params or {}

    def execute(self, *args: Any, **kwargs: Any) -> None:
        """Execute the step and cache the result in memory."""
        # Bind args/kwargs to parameter names using original signature
        bound = inspect.signature(self.step.execute).bind(*args, **kwargs)

        init_params = self.step.__dict__.copy()
        # Merge init parameters with execute parameters
        bound.arguments.update(init_params)
        bound.apply_defaults()

        # Serialize input arguments with cloudpickle
        try:
            serialized = cloudpickle.dumps(bound.arguments)
        except Exception as e:
            raise ValueError(f"Failed to serialize for cache: {e}")

        # Generate a hash key from inputs
        hash_key = hashlib.sha256(serialized).hexdigest()

        # Load from cache or compute and save
        if hash_key in self.cache:
            print(f"Loading cached result for {self.step.name} from memory")
            return self.cache[hash_key]
        else:
            print(
                f"Cache miss for {self.step.name}, executing step and saving result in memory"
            )
            result = self.step.execute(*args, **kwargs)
            self.cache[hash_key] = result
            return result

    def get_execute_params(self) -> Dict[str, Any]:
        """
        Get the parameters for the execute method of the wrapped step.
        """
        return self._execute_params

    @property
    def name(self) -> str:
        """
        Get the name of the step.
        """
        return self.step.name


class CachedPipelineMixin:
    def in_disk_cache(self, cache_dir: str = ".cache") -> Self:
        """
        It activate the in-disk cache using the InDisKCache class. returns the step itself.
        Args:
            cache_dir (str): Directory where the cache will be stored.
        """
        execute_params = self.get_execute_params()
        return InDiskCacheWrapper(
            self, cache_dir=cache_dir, execute_params=execute_params
        )

    def in_memory_cache(self) -> Self:
        """
        It activate the in-memory cache using the InMemoryCache class. returns the step itself.
        """
        execute_params = self.get_execute_params()
        return InMemoryCacheWrapper(self, execute_params=execute_params)
