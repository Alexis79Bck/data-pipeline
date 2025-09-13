# data-pipeline/test/test_utils.py
"""Tests for utility functions in common.utils module."""

import tempfile
from pathlib import Path

import pytest

from common.utils import (
    convert_time_12h_to_24h,
    load_from_json,
    parse_spanish_date,
    save_to_json,
    ValidationError,
    DataProcessingError,
)


class TestParseSpanishDate:
    """Test cases for parse_spanish_date function."""

    @pytest.mark.parametrize("input_str,expected", [
        ("6 de septiembre de 2025", "2025-09-06"),
        ("15 de enero de 2024", "2024-01-15"),
        ("1 de diciembre de 2023", "2023-12-01"),
        ("31 de mayo de 2022", "2022-05-31"),
        ("6 DE SEPTIEMBRE DE 2025", "2025-09-06"),  # Case insensitive
        ("6 De Septiembre De 2025", "2025-09-06"),  # Mixed case
    ])
    def test_valid_dates(self, input_str, expected):
        """Test parsing of valid Spanish date strings."""
        assert parse_spanish_date(input_str) == expected

    @pytest.mark.parametrize("input_str", [
        "fecha inválida",
        "6 de marzoo de 2025",  # Typo in month
        "6 de 13 de 2025",      # Invalid month number
        "",
        "6 de septiembre",      # Missing year
        "septiembre de 2025",   # Missing day
    ])
    def test_invalid_dates(self, input_str):
        """Test parsing of invalid Spanish date strings."""
        assert parse_spanish_date(input_str) is None

    def test_invalid_dates_none(self):
        """Test parsing with None input."""
        # The function catches ValidationError and logs it, returning None
        result = parse_spanish_date(None)
        assert result is None

    def test_invalid_dates_edge_cases(self):
        """Test edge cases that the function now validates."""
        # The function now validates ranges, so these should return None
        assert parse_spanish_date("32 de enero de 2025") is None  # Invalid day
        assert parse_spanish_date("0 de enero de 2025") is None   # Invalid day

    def test_all_months(self):
        """Test all Spanish months are correctly parsed."""
        months = [
            "enero", "febrero", "marzo", "abril", "mayo", "junio",
            "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre"
        ]
        for i, month in enumerate(months, 1):
            date_str = f"15 de {month} de 2024"
            expected = f"2024-{i:02d}-15"
            assert parse_spanish_date(date_str) == expected


class TestConvertTime12hTo24h:
    """Test cases for convert_time_12h_to_24h function."""

    @pytest.mark.parametrize("input_str,expected", [
        ("08:00 AM", "08:00:00"),
        ("08:00 PM", "20:00:00"),
        ("12:00 AM", "00:00:00"),
        ("12:00 PM", "12:00:00"),
        ("01:30 AM", "01:30:00"),
        ("11:59 PM", "23:59:00"),
        ("12:30:45 AM", "00:30:45"),  # With seconds
        ("12:30:45 PM", "12:30:45"),  # With seconds
    ])
    def test_valid_times(self, input_str, expected):
        """Test conversion of valid 12-hour time strings."""
        assert convert_time_12h_to_24h(input_str) == expected

    @pytest.mark.parametrize("input_str", [
        "invalid",
        "12:00",         # Missing AM/PM
        "12:00 XX",      # Invalid period
        "",
    ])
    def test_invalid_times(self, input_str):
        """Test conversion of invalid time strings."""
        assert convert_time_12h_to_24h(input_str) is None

    def test_invalid_times_none(self):
        """Test conversion with None input."""
        # The function catches ValidationError and logs it, returning None
        result = convert_time_12h_to_24h(None)
        assert result is None

    def test_invalid_times_edge_cases(self):
        """Test edge cases that the function now validates."""
        # The function now validates ranges, so these should return None
        assert convert_time_12h_to_24h("25:00 AM") is None  # Invalid hour
        assert convert_time_12h_to_24h("12:60 AM") is None  # Invalid minute
        assert convert_time_12h_to_24h("12:00:60 AM") is None  # Invalid second

    def test_case_insensitive(self):
        """Test that AM/PM case is handled correctly."""
        assert convert_time_12h_to_24h("12:00 am") == "00:00:00"
        assert convert_time_12h_to_24h("12:00 pm") == "12:00:00"
        assert convert_time_12h_to_24h("12:00 Am") == "00:00:00"
        assert convert_time_12h_to_24h("12:00 Pm") == "12:00:00"


class TestJsonUtils:
    """Test cases for JSON utility functions."""

    def test_save_and_load_json(self):
        """Test saving and loading JSON data."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "data.json"
            data = {"key": "value", "number": 42, "list": [1, 2, 3]}
            
            save_to_json(data, filepath)
            loaded = load_from_json(filepath)
            
            assert loaded == data
            assert isinstance(loaded, dict)
            assert filepath.exists()

    def test_save_json_creates_directory(self):
        """Test that save_to_json creates parent directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "nested" / "dir" / "data.json"
            data = {"test": "data"}
            
            save_to_json(data, filepath)
            
            assert filepath.exists()
            assert filepath.parent.exists()

    def test_load_nonexistent_json(self):
        """Test loading from non-existent JSON file."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "nonexistent.json"
            
            result = load_from_json(filepath)
            
            assert result is None

    def test_save_json_with_unicode(self):
        """Test saving JSON with Unicode characters."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "unicode.json"
            data = {"spanish": "café", "emoji": "🚀", "accent": "niño"}
            
            save_to_json(data, filepath)
            loaded = load_from_json(filepath)
            
            assert loaded == data
            assert loaded["spanish"] == "café"
            assert loaded["emoji"] == "🚀"

    def test_save_json_empty_data(self):
        """Test saving empty data structures."""
        with tempfile.TemporaryDirectory() as tmpdir:
            filepath = Path(tmpdir) / "empty.json"
            
            # Test empty dict
            save_to_json({}, filepath)
            assert load_from_json(filepath) == {}
            
            # Test empty list
            save_to_json([], filepath)
            assert load_from_json(filepath) == []
            
            # Test None - should raise DataProcessingError (wrapped ValidationError)
            with pytest.raises(DataProcessingError):
                save_to_json(None, filepath)