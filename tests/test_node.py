from typing import Any

import pytest

from linepipe.node import Node


def test_node_single_output() -> None:
    def add(a: int, b: int) -> int:
        return a + b

    node = Node(
        func=add,
        inputs=["a", "b"],
        outputs=["sum"],
    )

    result = node.run({"a": 2, "b": 3})
    assert result == {"sum": 5}


def test_node_multiple_outputs() -> None:
    def split(x: int) -> tuple[int, int]:
        return x, x * 2

    node = Node(
        func=split,
        inputs=["x"],
        outputs=["a", "b"],
    )

    result = node.run({"x": 4})
    assert result == {"a": 4, "b": 8}


def test_node_no_outputs() -> None:
    called = {"flag": False}

    def side_effect(x: int) -> None:
        called["flag"] = True

    node = Node(
        func=side_effect,
        inputs=["x"],
        outputs=[],
    )

    result = node.run({"x": 1})
    assert result == {}
    assert called["flag"] is True


def test_node_exception_wrapped() -> None:
    def boom(x: Any) -> None:
        raise ValueError("bad")

    node = Node(
        func=boom,
        inputs=["x"],
        outputs=["y"],
    )

    with pytest.raises(RuntimeError) as exc:
        node.run({"x": 1})

    assert "boom" in str(exc.value)
