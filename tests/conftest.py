import pytest
from pathlib import Path


@pytest.fixture(scope='session')
def data_dir():
    tests_dir = Path(__file__).resolve().parent
    yield tests_dir / "data"
