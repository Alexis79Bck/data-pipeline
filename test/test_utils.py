import pytest
from common.utils import parse_spanish_date, convert_time_12h_to_24h

def test_parse_spanish_date():
    assert parse_spanish_date("6 de septiembre de 2025") == "2025-09-06"
    assert parse_spanish_date("15 de enero de 2024") == "2024-01-15"
    assert parse_spanish_date("fecha inválida") is None

def test_convert_time_12h_to_24h():
    assert convert_time_12h_to_24h("08:00 AM") == "08:00:00"
    assert convert_time_12h_to_24h("08:00 PM") == "20:00:00"
    assert convert_time_12h_to_24h("12:00 AM") == "00:00:00"
    assert convert_time_12h_to_24h("12:00 PM") == "12:00:00"
