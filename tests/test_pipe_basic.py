import pytest

from linepipe.node import Node
from linepipe.pipeline import Pipeline


class Inner:
    def __init__(self) -> None:
        self.value = 99


class Config:
    def __init__(self) -> None:
        self.multiplier = 3
        self.nested = {"ab": 123, "deep": {"value": 42}}
        self.inner = Inner()
        self.mapping = {"key": 777}


def test_pipeline_simple_execution() -> None:
    def multiply(x: int, m: int) -> int:
        return x * m

    def add_one(x: int) -> int:
        return x + 1

    nodes = [
        Node(
            func=multiply,
            inputs=["add", "config.multiplier"],
            outputs=["y"],
        ),
        Node(
            func=add_one,
            inputs=["y"],
            outputs=["z"],
        ),
    ]

    pipeline = Pipeline(nodes=nodes, config=Config(), add=5, use_persistent_cache=True)

    pipeline.run()

    try:
        registry = pipeline.get_obj_registry()

        assert registry.get("y") == 15
        assert registry.get("z") == 16

    finally:
        registry.close()


def test_pipeline_addition() -> None:
    def f(x: int) -> int:
        return x + 1

    def g(y: int) -> int:
        return y * 2

    p1 = Pipeline(nodes=[Node(f, inputs=["x"], outputs=["y"])], config={}, x=3, use_persistent_cache=True)

    p2 = Pipeline(
        nodes=[Node(g, inputs=["y"], outputs=["z"])],
        config={},
    )

    pipeline = p1 + p2
    pipeline.run()

    try:
        registry = pipeline.get_obj_registry()

        assert registry.get("z") == 8

    finally:
        registry.close()


def test_pipeline_output_name_collision() -> None:
    def f(x: int) -> int:
        return x

    p1 = Pipeline(
        nodes=[Node(f, inputs=["x"], outputs=["y"])],
        config={},
        x=1,
    )

    p2 = Pipeline(
        nodes=[Node(f, inputs=["y"], outputs=["y"])],
        config={},
    )

    with pytest.raises(ValueError):
        _ = p1 + p2


def test_pipeline_config_nested_dict() -> None:
    """Access a value inside a dict attribute on config via dot notation."""

    def use_nested(val: int) -> int:
        return val + 1

    nodes = [
        Node(
            func=use_nested,
            inputs=["config.nested.ab"],
            outputs=["result"],
        ),
    ]

    pipeline = Pipeline(nodes=nodes, config=Config(), use_persistent_cache=True)
    pipeline.run()

    try:
        registry = pipeline.get_obj_registry()
        assert registry.get("result") == 124
    finally:
        registry.close()


def test_pipeline_config_deeply_nested_dict() -> None:
    """Access a value two levels deep inside nested dicts."""

    def use_deep(val: int) -> int:
        return val * 2

    nodes = [
        Node(
            func=use_deep,
            inputs=["config.nested.deep.value"],
            outputs=["result"],
        ),
    ]

    pipeline = Pipeline(nodes=nodes, config=Config(), use_persistent_cache=True)
    pipeline.run()

    try:
        registry = pipeline.get_obj_registry()
        assert registry.get("result") == 84
    finally:
        registry.close()


def test_pipeline_config_nested_attr() -> None:
    """Access a value on a nested attribute object (pydantic-like)."""

    def use_inner(val: int) -> int:
        return val - 9

    nodes = [
        Node(
            func=use_inner,
            inputs=["config.inner.value"],
            outputs=["result"],
        ),
    ]

    pipeline = Pipeline(nodes=nodes, config=Config(), use_persistent_cache=True)
    pipeline.run()

    try:
        registry = pipeline.get_obj_registry()
        assert registry.get("result") == 90
    finally:
        registry.close()


def test_pipeline_config_mixed_attr_and_dict() -> None:
    """Access a dict value through an attribute object."""

    def use_mapped(val: int) -> int:
        return val + 3

    nodes = [
        Node(
            func=use_mapped,
            inputs=["config.mapping.key"],
            outputs=["result"],
        ),
    ]

    pipeline = Pipeline(nodes=nodes, config=Config(), use_persistent_cache=True)
    pipeline.run()

    try:
        registry = pipeline.get_obj_registry()
        assert registry.get("result") == 780
    finally:
        registry.close()


def test_pipeline_config_missing_nested_key() -> None:
    """Missing nested key should use default (None) and not raise during attrgetter call."""
    from linepipe.pipeline import attrgetter

    getter = attrgetter("nonexistent.key", default=None)
    cfg = Config()
    assert getter(cfg) is None
