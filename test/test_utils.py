import pytest
from pathlib import Path
import tempfile
import json
from common.utils import parse_spanish_date, convert_time_12h_to_24h, save_to_json, load_from_json

@pytest.mark.parametrize("input_str,expected", [
    ("6 de septiembre de 2025", "2025-09-06"),
    ("15 de enero de 2024", "2024-01-15"),
    ("6 DE SEPTIEMBRE DE 2025", "2025-09-06"),
    ("fecha inválida", None),
    ("6 de marzoo de 2025", None),
    ("", None),
])
def test_parse_spanish_date(input_str, expected):
    assert parse_spanish_date(input_str) == expected

@pytest.mark.parametrize("input_str,expected", [
    ("08:00 AM", "08:00:00"),
    ("08:00 PM", "20:00:00"),
    ("12:00 AM", "00:00:00"),
    ("12:00 PM", "12:00:00"),
    ("invalid", None),
])
def test_convert_time_12h_to_24h(input_str, expected):
    assert convert_time_12h_to_24h(input_str) == expected

def test_json_utils():
    with tempfile.TemporaryDirectory() as tmpdir:
        filepath = Path(tmpdir) / "data.json"
        data = {"key": "value"}
        
        save_to_json(data, filepath)
        loaded = load_from_json(filepath)
        
        assert loaded == data
        assert isinstance(loaded, dict)
