from collections.abc import Callable
from functools import partial
from logging import getLogger
from typing import Any, cast

logger = getLogger(__name__)


class Node:
    def __init__(self, func: Callable[..., Any], inputs: list[str], outputs: list[str], profile: bool = False):
        self.func = func
        self.inputs = inputs
        self.outputs = outputs
        self.profile = profile

    def run(self, data: dict[str, Any]) -> dict[str, Any]:
        func_args = [data[key] for key in self.inputs]

        def _run_func() -> Any:
            try:
                return self.func(*func_args)
            except Exception as e:
                raise RuntimeError(f"An error occurred while running {self.func.__name__}") from e

        if self.profile:
            mem_usage, result = self._profile_memory(_run_func)
            logger.info(f"[{self.func.__name__}] ΔMem: {mem_usage['delta']:.2f} MiB | " f"Peak: {mem_usage['peak']:.2f} MiB")
        else:
            result = _run_func()

        if not self.outputs:
            return {}

        if len(self.outputs) == 1:
            return {self.outputs[0]: result}

        return dict(zip(self.outputs, result, strict=False))

    def _profile_memory(self, func: Callable[[], Any]) -> tuple[dict[str, float], Any]:
        try:
            from memory_profiler import memory_usage  # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            raise RuntimeError(
                "Memory profiling is enabled, but 'memory_profiler' is not installed. "
                "Install with: pip install linepipe[memory]"
            ) from exc

        # result_container = {}

        # def wrapper() -> None:
        #     result_container["output"] = func()

        (mem_values, result) = memory_usage((func, (), {}), interval=0.05, retval=True)  # pyright: ignore[reportArgumentType]
        delta = mem_values[-1] - mem_values[0]
        peak = max(mem_values) - mem_values[0]

        return {"delta": delta, "peak": peak}, result


def create_named_partial_function(func: Callable[..., Any], func_name: str, **kwargs: dict[str, Any]) -> Callable[..., Any]:
    f = cast(Callable[[], Any], partial(func, **kwargs))
    f.__name__ = func_name
    f.__no_type_check__ = True  # type: ignore[attr-defined]
    return f
