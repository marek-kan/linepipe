import shelve
from enum import Enum, auto
from logging import getLogger
from pathlib import Path
from pickle import HIGHEST_PROTOCOL
from typing import Any

logger = getLogger(__name__)


class ObjPlacement(Enum):
    CONSTANT = auto()
    STORAGE = auto()
    RUNTIME = auto()


class ObjectRegistry:
    """
    ObjectRegistry manages the lifecycle and placement of objects produced and consumed during Pipeline execution.

    It is responsible for:
    - Managing persistent or in-memory storage of intermediate results
    - Holding runtime-only objects that cannot be pickled
    - Providing read-only runtime constants
    - Releasing objects deterministically when they are no longer needed

    The registry owns the storage backend and must be explicitly closed to release resources.
    """

    def __init__(
        self, cache_storage_path: str | Path, use_persistent_cache: bool, runtime_constants: dict[str, Any] | None = None
    ):
        self.cache_storage_path = cache_storage_path
        self.runtime_constants: dict[str, Any] = dict(runtime_constants) if runtime_constants else {}
        self.runtime_ephemeral: dict[str, Any] = {}
        self.use_persistent_cache = use_persistent_cache

        self.data_storage = self.open_cache()

        self.placement: dict[str, ObjPlacement] = {k: ObjPlacement.CONSTANT for k in self.runtime_constants}

        for k in self.data_storage.keys():  # noqa: SIM118
            if k in self.placement:
                raise ValueError(
                    f"Name collision: '{k}' exists in both runtime_constants and "
                    f"persistent cache at '{self.cache_storage_path}'"
                )

            self.placement[k] = ObjPlacement.STORAGE

        if self.use_persistent_cache:
            logger.info(
                f"Restored {sum(p is ObjPlacement.STORAGE for p in self.placement.values())} objects from persistent cache"
            )

        self._closed = False

    def open_cache(self) -> shelve.Shelf[Any] | dict[str, Any]:
        """
        Initialize and return the underlying storage backend.

        Returns:
            - A shelve.Shelf instance if persistent caching is enabled
            - An in-memory dict if persistence is disabled
        """

        if not self.use_persistent_cache:
            logger.info("Using in-memory cache storage")
            return {}  # dict is more light-weight if we want only in-memory data_storage registry

        if isinstance(self.cache_storage_path, str):
            self.cache_storage_path = Path(self.cache_storage_path)

        if not self.cache_storage_path.parent.exists():
            logger.info(f"Creating data storage directory: {self.cache_storage_path.parent}")
            self.cache_storage_path.parent.mkdir(parents=True)
        else:
            logger.info(f"Using data storage located at: {self.cache_storage_path.parent}")

        return shelve.open(str(self.cache_storage_path), protocol=HIGHEST_PROTOCOL, writeback=False)  # noqa: S301

    def get(self, key: str) -> Any:
        """
        Retrieve an object from the registry.

        Raises:
            KeyError: if the object does not exist
            RuntimeError: if the registry is closed
        """

        if self._closed:
            raise RuntimeError("ObjectRegistry is closed")

        placement = self.placement.get(key, None)

        if placement is ObjPlacement.CONSTANT:
            return self.runtime_constants[key]

        if placement is ObjPlacement.STORAGE:
            return self.data_storage[key]

        if placement is ObjPlacement.RUNTIME:
            return self.runtime_ephemeral[key]

        raise KeyError(f"Object '{key}' not found in registry")

    def set(self, key: str, value: Any) -> None:
        """
        Store an object in the registry.

        The registry will attempt to persist the object in storage. If persistence
        fails (e.g., object is not pickleable), the object is stored in runtime
        memory instead.

        Runtime constants cannot be overwritten.
        """

        if self._closed:
            raise RuntimeError("ObjectRegistry is closed")

        if key in self.runtime_constants:
            logger.error(f"Trying to rewrite constant '{key}'! This is not allowed, skipping.")
            return

        try:
            logger.info(f"Storing output '{key}' in data storage.")

            self.data_storage[key] = value
            self.placement[key] = ObjPlacement.STORAGE

        except Exception as e:
            logger.warning(
                f"Failed to store output '{key}' in data storage "
                f"({type(e).__name__}: {e}). Using runtime memory instead!"
            )

            self.runtime_ephemeral[key] = value
            self.placement[key] = ObjPlacement.RUNTIME

    def flush(self) -> None:
        """Sync memory with persistent storage"""

        sync = getattr(self.data_storage, "sync", None)
        if sync:
            sync()

    def release(self, key: str) -> None:
        """ "Reseales objects from `data_storage` or `runtime_ephemeral`. Constants are intentionally left out"""

        placement = self.placement.get(key, None)

        if placement is None:
            return

        if placement is ObjPlacement.RUNTIME:
            logger.info(f"Releasing '{key}' from {ObjPlacement.RUNTIME.name}")
            self.runtime_ephemeral.pop(key, None)
            self.placement.pop(key, None)

        elif placement is ObjPlacement.STORAGE:
            if not self.use_persistent_cache:
                logger.info(f"Releasing '{key}' from {ObjPlacement.STORAGE.name}")
                self.data_storage.pop(key, None)
                self.placement.pop(key, None)

    def close(self) -> None:
        """
        Close the registry and release all managed resources.

        After closing, no further get/set operations are allowed. This method is idempotent.
        """
        if self._closed:
            return

        self.flush()

        if isinstance(self.data_storage, shelve.Shelf):
            self.data_storage.close()

        if isinstance(self.data_storage, dict):
            self.data_storage.clear()

        self.runtime_ephemeral.clear()
        self.placement.clear()
        self._closed = True

    def has(self, key: str) -> bool:
        """
        Raises:
            RuntimeError: if the registry is closed
        """
        if self._closed:
            raise RuntimeError("ObjectRegistry is closed")

        return key in self.placement
