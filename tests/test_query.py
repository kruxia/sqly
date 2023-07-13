import pytest

from sqly import Q
from tests import fixtures


@pytest.mark.parametrize("fields", fixtures.fields)
def test_q_keys(fields):
    result = Q.keys(fields)
    assert len(result) == len(fields)
    assert not any(":" in field for field in fields)


@pytest.mark.parametrize("fields", fixtures.fields)
def test_q_fields(fields):
    result = Q.fields(fields)
    assert isinstance(result, str)
    assert len(result.split(",")) == len(fields)
    assert Q.fields(fields) == ", ".join(fields)
    assert ":" not in fields


@pytest.mark.parametrize("fields", fixtures.fields)
def test_q_params(fields):
    result = Q.params(fields)
    assert isinstance(result, str)
    assert len(result.split(",")) == len(fields)
    assert result.count(":") == len(fields)


@pytest.mark.parametrize("fields", fixtures.fields)
def test_q_assigns(fields):
    result = Q.assigns(fields)
    assert isinstance(result, str)
    assert len(result.split(",")) == len(fields)
    assert result.count("=") == len(fields)


@pytest.mark.parametrize("fields", fixtures.fields)
def test_q_filter(fields):
    for field in fields:
        op = fixtures.field_filter_ops.get(field)
        kwargs = {}
        if op:
            kwargs["op"] = op
        result = Q.filter(field, **kwargs)
        assert isinstance(result, str)
        assert f" {op or '='} " in result
        assert result.startswith(field)
        assert result.endswith(f":{field}")
