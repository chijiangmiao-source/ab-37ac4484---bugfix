from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app


@pytest.fixture()
def data_dir(tmp_path):
    return tmp_path / "data"


@pytest.fixture()
def client(data_dir):
    app = create_app(Settings(data_dir=data_dir))
    with TestClient(app) as test_client:
        yield test_client
