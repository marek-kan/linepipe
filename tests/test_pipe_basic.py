import pytest

from linepipe.node import Node
from linepipe.pipeline import Pipeline


class Config:
    def __init__(self) -> None:
        self.multiplier = 3


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

    pipeline = Pipeline(nodes=nodes, config=Config(), add=5, use_persistant_cache=True)

    pipeline.run()

    try:
        ds = pipeline._initialize_cache_storage()

        assert ds["y"] == 15
        assert ds["z"] == 16

    finally:
        ds.close()


def test_pipeline_addition() -> None:
    def f(x: int) -> int:
        return x + 1

    def g(y: int) -> int:
        return y * 2

    p1 = Pipeline(nodes=[Node(f, inputs=["x"], outputs=["y"])], config={}, x=3, use_persistant_cache=True)

    p2 = Pipeline(
        nodes=[Node(g, inputs=["y"], outputs=["z"])],
        config={},
    )

    pipeline = p1 + p2
    pipeline.run()

    try:
        ds = pipeline._initialize_cache_storage()

        assert ds["z"] == 8
    finally:
        ds.close()


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
