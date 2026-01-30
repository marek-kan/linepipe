from collections import defaultdict
from copy import deepcopy
from logging import getLogger
from pathlib import Path
from typing import Any, get_args, get_type_hints

from linepipe.node import Node
from linepipe.registry import ObjectRegistry

logger = getLogger(__name__)


class attrgetter:
    """
    Return a callable object that fetches the given attribute(s) from its operand.
    After f = attrgetter('name'), the call f(r) returns r.name.
    After g = attrgetter('name', 'date'), the call g(r) returns (r.name, r.date).
    After h = attrgetter('name.first', 'name.last'), the call h(r) returns
    (r.name.first, r.name.last).
    """

    __slots__ = ("_attrs", "_call", "_default")

    def __init__(self, attr: str, *attrs: str, default: Any | None = None):
        self._default = default

        if not attrs:
            if not isinstance(attr, str):
                raise TypeError("attribute name must be a string")
            self._attrs: tuple[str, ...] = (attr,)
            names = attr.split(".")

            def func(obj: Any) -> Any:
                for name in names:
                    obj = getattr(obj, name)
                return obj

        else:
            self._attrs = (attr,) + attrs
            getters = tuple(attrgetter(a, default=self._default) for a in self._attrs)

            def func(obj: Any) -> Any:
                return tuple(getter(obj) for getter in getters)

        self._call = func

    def __call__(self, obj: Any) -> Any:
        try:
            return self._call(obj)
        except AttributeError:
            return self._default

    def __repr__(self) -> str:
        names = ", ".join(map(repr, self._attrs))
        return f"{self.__class__.__name__}({names}, default={self._default!r})"

    def __reduce__(self) -> tuple[type["attrgetter"], tuple[str, ...], str | None]:
        return self.__class__, self._attrs, self._default


class Pipeline:
    """
    A sequential execution pipeline composed of Node objects.

    The Pipeline is responsible for:
    - Orchestrating node execution order
    - Resolving node inputs by name (config vs registry-managed objects)
    - Coordinating lifecycle of an ObjectRegistry per run
    - Optional execution history tracking for debugging

    Pipelines can be composed using the `+` operator.
    """

    def __init__(
        self,
        nodes: list[Node],
        config: Any = None,
        track_history: bool = False,
        use_persistent_cache: bool = False,
        cache_storage_path: str | Path = Path("./.cache/data_storage/"),
        **kwargs: Any,
    ) -> None:
        """
        Args:
            nodes: list of functions wrapped in `linepipe.node.Node`
            config: dot-accessible config, i.e., it is possible to get some value as `config_obj.key`
            track_history:
                If True, execution history is stored in-memory as a list of dicts.
                Each entry contains the node name, inputs, and outputs.
                Inputs and outputs are deep-copied to preserve snapshots, which may lead
                to significant memory usage for large objects.
            use_persistent_cache: if to persist data_storage on disk.
            cache_storage_path: where to persist data_storage in case of `use_persistent_cache`
            **kwargs: any runtime constants, variables that are not present in config. For example, DB engine or such.
        """
        self.nodes = nodes
        self.track_history = track_history
        self.history: list[dict[str, Any]] = []
        self.config = deepcopy(config)
        self.cache_storage_path = cache_storage_path
        self.use_persistent_cache = use_persistent_cache
        self.runtime_constants = {k: self._snapshot(value=v, key=k) for k, v in kwargs.items()}

    @property
    def output_hints(self) -> dict[str, Any]:
        """
        Infer output types for pipeline outputs based on node return annotations.

        Returns:
            Mapping from output variable name to its annotated type.

        Notes:
            - For nodes with a single output, the function return type is used directly.
            - For nodes with multiple outputs, the return type must be a tuple type.
            - Missing or incorrect annotations will raise a ValueError or IndexError.
        """
        hints = {}
        for node in self.nodes:
            if not node.outputs:
                continue

            return_type = get_type_hints(node.func).get("return", None)

            if return_type is None:
                logger.warning(f"Function `{node.func.__name__}` has signature return type `None`!")

            if len(node.outputs) == 1:
                hints[node.outputs[0]] = return_type
            else:
                return_tuple = get_args(return_type)

                if not return_tuple:
                    raise ValueError(f"Function `{node.func.__name__}` with multiple outputs is incorrectly annotated!")

                if Ellipsis in return_tuple:
                    raise ValueError(f"Function `{node.func.__name__}` must use a fixed-length tuple annotation")

                for i, output in enumerate(node.outputs):
                    try:
                        hints[output] = return_tuple[i]
                    except IndexError as err:
                        raise IndexError(
                            f"Function `{node.func.__name__}` has wrong number of annotated outputs! "
                            + f"Pipeline outputs: {len(node.outputs)}, annotated outputs: {len(return_tuple)}"
                        ) from err
        return hints

    @property
    def node_order(self) -> list[str]:
        return [node.func.__name__ for node in self.nodes]

    @property
    def nodes_info(self) -> list[dict[str, Any]]:
        return [{"func": node.func.__name__, "inputs": node.inputs, "outputs": node.outputs} for node in self.nodes]

    def _build_cleanup_plan(self) -> dict[int, list[str]]:
        last_use: dict[str, int] = {}

        for nth, node in enumerate(self.nodes_info):
            for inp in node["inputs"]:
                last_use[inp] = nth

        cleanup = defaultdict(list)
        for key, nth in last_use.items():
            if key.startswith("config.") or key == "config":
                continue

            cleanup[nth].append(key)

        return dict(cleanup)

    def get_obj_registry(self) -> ObjectRegistry:
        return ObjectRegistry(
            cache_storage_path=self.cache_storage_path,
            use_persistent_cache=self.use_persistent_cache,
            runtime_constants=self.runtime_constants,
        )

    @staticmethod
    def _snapshot(value: Any, key: str) -> Any:
        try:
            return deepcopy(value)
        except Exception:
            logger.warning(f"Couldn't deepcopy '{key}'; using shallow reference")
            return value

    def run(self) -> None:
        """
        Execute the pipeline by processing nodes sequentially.

        For each node, the pipeline:

        1. Resolves required inputs:
            - Inputs prefixed with "config." or equal to "config" are taken from the pipeline configuration and deep-copied.
            - All other inputs are retrieved from the ObjectRegistry and deep-copied.
            - A KeyError is raised if a required input is missing.

        2. Invokes the node with the resolved inputs.

        3. Stores node outputs via the ObjectRegistry, which decides whether outputs are persisted or kept in runtime memory.

        4. Optionally records execution history by storing deep-copied snapshots of node inputs and outputs.

        5. Releases intermediate objects according to the cleanup plan.

        All registry resources are flushed and closed when execution finishes, even if an exception occurs.
        """

        cleanup_plan = self._build_cleanup_plan()
        registry = self.get_obj_registry()

        try:
            for nth_node, node in enumerate(self.nodes):
                logger.info(f"Running node: {node.func.__name__}")

                # Record the inputs for the current node.
                node_inputs = {}
                for key in node.inputs:
                    if key.startswith("config."):
                        node_inputs[key] = deepcopy(attrgetter(key.replace("config.", ""))(self.config))
                    elif key == "config":
                        node_inputs[key] = deepcopy(self.config)
                    else:
                        if not registry.has(key):
                            raise KeyError(f"Input '{key}' not found for node {node.func.__name__} in the registry.")

                        node_inputs[key] = self._snapshot(value=registry.get(key), key=key)

                if self.track_history:
                    hist_dict = {
                        "node": node.func.__name__,
                        "inputs": {k: self._snapshot(value=v, key=k) for k, v in node_inputs.items()},
                    }

                output = node.run(node_inputs)

                if not isinstance(output, dict):
                    raise ValueError(f"Output of node is not a dict! {type(output)=}")

                for k, v in output.items():
                    registry.set(key=k, value=v)

                registry.flush()

                if self.track_history:
                    hist_dict["outputs"] = {k: self._snapshot(value=v, key=k) for k, v in output.items()}
                    self.history.append(hist_dict)
                else:
                    node_inputs.clear()
                    output.clear()

                for k in cleanup_plan.get(nth_node, []):
                    registry.release(key=k)

        finally:
            registry.close()
        return

    def __add__(self, other: "Pipeline") -> "Pipeline":
        """
        Overloads the + operator to allow combining two pipelines.
        The resulting pipeline contains the nodes from self followed by those from other.
        The configuration of self is used.
        """
        if not isinstance(other, Pipeline):
            return TypeError(f"Can only combine two Pipelines, other is type: {type(other)}")

        # Combine nodes from both pipelines.
        combined_nodes = self.nodes + other.nodes

        # Determine track_history setting.
        new_track_history = self.track_history or other.track_history

        output_names = set(self.output_hints.keys()).intersection(other.output_hints.keys())
        if output_names:
            raise ValueError(f"Output names {output_names} are present in both Pipelines and cannot be combined!")

        runtime_variables = {**self.runtime_constants, **other.runtime_constants}

        new_pipeline = Pipeline(
            nodes=combined_nodes,
            config=deepcopy(self.config),
            track_history=new_track_history,
            cache_storage_path=self.cache_storage_path,
            use_persistent_cache=any([self.use_persistent_cache, other.use_persistent_cache]),
            **runtime_variables,
        )

        return new_pipeline
