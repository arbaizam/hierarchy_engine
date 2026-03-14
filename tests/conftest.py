import pytest

from tests.helpers import build_definition


@pytest.fixture
def definition():
    return build_definition()
