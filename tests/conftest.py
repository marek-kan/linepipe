from typing import Any

import pytest


class Inner:
    def __init__(self) -> None:
        self.value = 99


class Config:
    def __init__(self) -> None:
        self.multiplier = 3
        self.nested = {"ab": 123, "deep": {"value": 42}}
        self.inner = Inner()
        self.mapping = {"key": 777}


@pytest.fixture  # type: ignore[misc]
def dict_config() -> dict[str, Any]:
    return Config().__dict__


@pytest.fixture  # type: ignore[misc]
def obj_config() -> Config:
    return Config()


@pytest.fixture  # type: ignore[misc]
def config(request: pytest.FixtureRequest) -> Config | dict[str, Any]:
    return request.getfixturevalue(request.param)  # type: ignore[no-any-return]
