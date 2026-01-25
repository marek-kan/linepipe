import logging

import pytest

from linepipe.node import Node
from linepipe.pipeline import Pipeline

pytest.importorskip("memory_profiler")


def test_mem_profile(caplog: pytest.LogCaptureFixture) -> None:
    def add(x: int, y: int) -> int:
        return x + y

    pipeline = Pipeline(nodes=[Node(add, inputs=["x", "y"], outputs=["z"], profile=True)], x=1, y=2)

    with caplog.at_level(logging.INFO):
        pipeline.run()

    log_text = caplog.text

    assert "ΔMem:" in log_text
    assert "Peak:" in log_text
