import pytest
from pathlib import Path
import pandas as pd



@pytest.fixture(scope='session')
def data_dir():
    tests_dir = Path(__file__).resolve().parent
    yield tests_dir / "data"


@pytest.fixture(scope='session')
def vbo_df(data_dir):
    file = data_dir / "vbo.csv"
    return pd.read_csv(file, index_col=["vbo_identificatie"])
