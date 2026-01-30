from linepipe.node import Node
from linepipe.pipeline import Pipeline


def test_pipeline_history_records_inputs_and_outputs() -> None:
    def multiply(x: int, m: int) -> int:
        return x * m

    nodes = [
        Node(
            func=multiply,
            inputs=["add", "multiplier"],
            outputs=["y"],
        ),
    ]

    pipeline = Pipeline(
        nodes=nodes,
        add=5,
        multiplier=3,
        track_history=True,
    )

    pipeline.run()

    assert len(pipeline.history) == 1

    entry = pipeline.history[0]

    assert entry["node"] == "multiply"
    assert entry["inputs"]["add"] == 5
    assert entry["inputs"]["multiplier"] == 3
    assert entry["outputs"]["y"] == 15


def test_pipeline_history_is_not_affected_by_later_mutation() -> None:
    def mutate(x: dict[str, int]) -> dict[str, dict[str, int]]:
        x["value"] += 1
        return {"out": x}

    shared = {"value": 1}

    nodes = [
        Node(
            func=mutate,
            inputs=["x"],
            outputs=["y"],
        ),
    ]

    pipeline = Pipeline(
        nodes=nodes,
        x=shared,
        track_history=True,
    )

    pipeline.run()

    # mutate original after run
    shared["value"] = 999

    hist_input = pipeline.history[0]["inputs"]["x"]
    hist_output = pipeline.history[0]["outputs"]["y"]

    assert hist_input["value"] == 1
    assert hist_output["out"]["value"] == 2


def test_pipeline_history_non_deepcopyable_object() -> None:
    import threading

    def identity(x: threading.Lock) -> dict[str, threading.Lock]:
        return {"out": x}

    lock = threading.Lock()

    nodes = [
        Node(
            func=identity,
            inputs=["x"],
            outputs=["y"],
        ),
    ]

    pipeline = Pipeline(
        nodes=nodes,
        x=lock,
        track_history=True,
    )

    pipeline.run()

    entry = pipeline.history[0]

    assert entry["inputs"]["x"] is lock
    assert entry["outputs"]["y"]["out"] is lock
