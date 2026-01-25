import pickle
import shelve
from copy import deepcopy
from logging import getLogger
from pathlib import Path
from typing import Any, get_args, get_type_hints

from linepipe.node import Node

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

    The Pipeline manages:
    - Dependency resolution between node inputs and outputs
    - Optional persistent or in-memory caching of intermediate results
    - Runtime-only objects that cannot be pickled
    - Optional execution history tracking for debugging

    Pipelines can be composed using the `+` operator.
    """

    def __init__(
        self,
        nodes: list[Node],
        config: Any = None,
        track_history: bool = False,
        cache_storage_path: str | Path = Path("./.cache/data_storage/"),
        use_persistent_cache: bool = False,
        **kwargs: Any,
    ) -> None:
        """
        Args:
            nodes: list of functions wrapped in `linepipe.node.Node`
            config: dot-accessible config, i.e., it is possible to get some value as `config_obj.key`
            track_history: if to track history in-memory dict. If True, self.history: list[dict] contains the node's name, inputs, and outputs.
            cache_storage_path: where to persist data_storage: shelve.Shelf
            use_persistent_cache: if to persist data_storage on disk.
            **kwargs: any runtime constants, variables that are not present in config. For example, DB engine or such.
        """
        self.nodes = nodes
        self.track_history = track_history
        self.history: list[dict[str, Any]] = []
        self.config = deepcopy(config)
        self.cache_storage_path = cache_storage_path
        self.use_persistent_cache = use_persistent_cache
        # Note: this is needed bc. not all the objects are pickle serializable, for example, sqlalchemy eng
        self.runtime_objs = deepcopy(kwargs)

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
                    raise ValueError(f"Function `{node.func.__name__}` with multiple outputs is incorectlly annotated!")

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

    def open_cache(self) -> shelve.Shelf[Any]:
        """
        Initialize cache storage.

        Returns:
            A shelve.Shelf instance backed by:
            - an in-memory dictionary if persistence is disabled
            - a filesystem-backed store otherwise
        """
        if not self.use_persistent_cache:
            logger.info("Using in-memory cache storage")
            return shelve.Shelf({})

        if isinstance(self.cache_storage_path, str):
            self.cache_storage_path = Path(self.cache_storage_path)

        if not self.cache_storage_path.parent.exists():
            logger.info(f"Creating data storage directory: {self.cache_storage_path.parent}")
            self.cache_storage_path.parent.mkdir(parents=True)
        else:
            logger.info(f"Using data storage located at: {self.cache_storage_path.parent}")

        return shelve.open(str(self.cache_storage_path), protocol=pickle.HIGHEST_PROTOCOL, writeback=False)  # noqa: S301

    def run(self) -> None:
        """
        Executes the pipeline by processing each node sequentially. For each node, the method:

        1. Gathers required inputs from three sources in the following order:
           - If the key starts with "config", the corresponding attribute from the Pipeline instance is deep-copied.
           - If the key exists in the (non)persistent data storage (using shelve), it is retrieved from there.
           - If the key exists in the runtime objects dictionary, it is retrieved from there.
           If an input key is missing from both storage and runtime objects, a KeyError is raised.

        2. Invokes the node's function with the collected inputs.

        3. Processes the outputs:
           - If the output is pickle-compatible, it is stored in the shelve data storage.
           - If storing fails (e.g., due to non-pickleable objects), the output is stored in the runtime objects and
             a warning is logged.
        4. If history tracking is enabled, logs the node's name, inputs, and outputs for debugging purposes.
        """
        self.data_storage = self.open_cache()
        try:
            for node in self.nodes:
                logger.info(f"Running node: {node.func.__name__}")

                # Record the inputs for the current node.
                node_inputs = {}
                for key in node.inputs:
                    if key.startswith("config."):
                        node_inputs[key] = deepcopy(attrgetter(key.replace("config.", ""))(self.config))
                    elif key == "config":
                        node_inputs[key] = deepcopy(self.config)
                    elif key in self.data_storage:
                        node_inputs[key] = deepcopy(self.data_storage[key])
                    elif key in self.runtime_objs:
                        node_inputs[key] = self.runtime_objs[key]
                    else:
                        raise KeyError(f"Input '{key}' not found in data storage or runtime objects.")

                output = node.run(node_inputs)

                if isinstance(output, dict):
                    for k, v in output.items():
                        try:
                            logger.info(f"Storing output '{k}' in data storage.")
                            self.data_storage[k] = v
                            self.data_storage.sync()

                            logger.info("Success")
                        except (TypeError, pickle.PicklingError):
                            logger.warning(f"Failed to store output '{k}' in data storage. Using runtime memory instead!")
                            self.runtime_objs[k] = v

                if self.track_history:
                    self.history.append(
                        {
                            "node": node.func.__name__,
                            "inputs": node_inputs,
                            "outputs": output,
                        }
                    )
        finally:
            self.data_storage.close()
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

        runtime_variables = {**self.runtime_objs, **other.runtime_objs}

        new_pipeline = Pipeline(
            nodes=combined_nodes,
            config=deepcopy(self.config),
            track_history=new_track_history,
            cache_storage_path=self.cache_storage_path,
            use_persistent_cache=any([self.use_persistent_cache, other.use_persistent_cache]),
            **runtime_variables,
        )

        return new_pipeline
