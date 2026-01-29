from collections.abc import Callable, Sequence
from functools import partial
from logging import getLogger
from typing import Any, cast

logger = getLogger(__name__)


class Node:
    """
    A pipeline execution unit wrapping a callable.

    A Node defines:
    - which input keys it consumes
    - which output keys it produces
    - whether execution should be memory-profiled

    Nodes are executed by Pipeline.run().
    """

    def __init__(self, func: Callable[..., Any], inputs: Sequence[str], outputs: Sequence[str], profile: bool = False):
        self.func = func
        self.inputs = tuple(inputs)
        self.outputs = tuple(outputs)
        self.profile = profile

    def run(self, data: dict[str, Any]) -> dict[str, Any]:
        func_args = [data[key] for key in self.inputs]

        def _invoke() -> Any:
            try:
                return self.func(*func_args)
            except Exception as e:
                raise RuntimeError(f"An error occurred while running {self.func.__name__}") from e

        if self.profile:
            mem_usage, result = self._profile_memory(_invoke)
            logger.info(f"[{self.func.__name__}] ΔMem: {mem_usage['delta']:.2f} MiB | " f"Peak: {mem_usage['peak']:.2f} MiB")
        else:
            result = _invoke()

        if not self.outputs:
            return {}

        if len(self.outputs) == 1:
            return {self.outputs[0]: result}

        if len(self.outputs) != len(result):
            raise ValueError(
                f"Function `{self.func.__name__}` returned {len(result)} values, "
                f"but {len(self.outputs)} outputs were declared"
            )

        return dict(zip(self.outputs, result, strict=False))

    def _profile_memory(self, func: Callable[[], Any]) -> tuple[dict[str, float], Any]:
        """This is a best-effort, sampling-based measurement intended for debugging and comparison, not exact accounting"""
        try:
            from memory_profiler import memory_usage  # pyright: ignore[reportMissingImports]
        except ImportError as exc:
            raise RuntimeError(
                "Memory profiling is enabled, but 'memory_profiler' is not installed. "
                "Install with: pip install linepipe[memory]"
            ) from exc

        baseline_values = memory_usage(-1, interval=0.05, timeout=0.5)

        mem_values, result = memory_usage((func, (), {}), interval=0.05, retval=True)  # pyright: ignore[reportArgumentType]
        baseline = sum(baseline_values) / len(baseline_values) if baseline_values else mem_values[0]

        delta = mem_values[-1] - mem_values[0]
        peak = max(mem_values) - baseline

        return {"delta": delta, "peak": peak}, result


def create_named_partial_function(func: Callable[..., Any], func_name: str, **kwargs: Any) -> Callable[..., Any]:
    """
    Create a partially-applied function with a stable, explicit name.

    This is primarily used to adapt generic writer or helper functions
    into distinct Pipeline nodes by:
      - pre-binding keyword arguments (e.g. schema, table_name)
      - overriding __name__ for clearer logging, history tracking,
        and pipeline introspection

    Example:
        write_stats = Node(
            func=create_named_partial_function(
                func=writers.write_pipeline_output,
                func_name="write_model_statistics",
                schema=SCHEMA,
                table_name=TABLE_NAMES["model_statistics"],
            ),
            inputs=["model_stats_df", "db_engine"],
            outputs=[],
        )

    Notes:
        - The returned callable behaves like the original function, but with fixed keyword arguments.
        - Type checking is intentionally disabled on the returned function
          to avoid false positives from functools.partial in pipeline contexts.
    """
    f = cast(Callable[[], Any], partial(func, **kwargs))
    f.__name__ = func_name
    f.__no_type_check__ = True  # type: ignore[attr-defined]
    return f
