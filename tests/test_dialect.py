import pytest

from sqly.dialect import Dialect, ParamFormat
from tests import fixtures


@pytest.mark.parametrize("dialect_name", fixtures.valid_dialect_names)
def test_dialect_import_adaptor(dialect_name):
    dialect = Dialect(dialect_name)
    mod = dialect.adaptor()
    assert mod.__name__ == dialect.adaptor_name
    assert dialect.param_format.is_keyed != dialect.param_format.is_positional


def test_dialect_keyed_positional():
    assert ParamFormat.NAMED.is_keyed
    assert ParamFormat.QMARK.is_positional
