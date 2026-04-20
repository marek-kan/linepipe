import pytest
from linepipe.pipeline import attrgetter
from .conftest import Config, obj_config, dict_config

@pytest.mark.parametrize("config", ["obj_config", "dict_config"], indirect=True)
def test_pipeline_config_missing_nested_key(config: Config | dict) -> None:
    """Missing nested key should use default (None) and not raise during attrgetter call."""
    
    getter = attrgetter("nonexistent.key", default=None)
    assert getter(config) is None
