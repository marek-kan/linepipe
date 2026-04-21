import pytest

from linepipe.pipeline import attrgetter

from .conftest import Config


@pytest.mark.parametrize("config", ["obj_config", "dict_config"], indirect=True)  # type: ignore[misc]
def test_config_missing_nested_key(config: Config | dict) -> None:  # type: ignore[type-arg]
    getter = attrgetter("nonexistent.key", default=None)
    assert getter(config) is None


@pytest.mark.parametrize("config", ["obj_config", "dict_config"], indirect=True)  # type: ignore[misc]
def test_config_multiple_keys(config: Config | dict) -> None:  # type: ignore[type-arg]
    getter = attrgetter("multiplier", "inner.value", default=None)
    assert getter(config) == (3, 99)


@pytest.mark.parametrize("config", ["obj_config", "dict_config"], indirect=True)  # type: ignore[misc]
def test_config_multiple_keys_one_missing(config: Config | dict) -> None:  # type: ignore[type-arg]
    getter = attrgetter("nested.deep.value", "inner.foo.boo", default=-1)
    assert getter(config) == (42, -1)
