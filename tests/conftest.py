from mock import MagicMock
import json
from os import path
import time

import pytest

from elastalert_extensions import ruletypes


@pytest.fixture
def mock_time(monkeypatch):
    mocker = MagicMock(name='time.time')
    monkeypatch.setattr(time, 'time', mocker)
    return mocker


@pytest.fixture
def mock_getmtime(monkeypatch):
    mocker = MagicMock(name='os.path.getmtime')
    monkeypatch.setattr(path, 'getmtime', mocker)
    return mocker


@pytest.fixture
def mock_json_load(monkeypatch):
    mocker = MagicMock(name='json.load')
    monkeypatch.setattr(json, 'load', mocker)
    return mocker


@pytest.fixture
def mock_ruletypes_open(monkeypatch):
    mocker = MagicMock(name='open')
    monkeypatch.setattr(ruletypes, 'open', mocker, raising=False)
    return mocker
