# data-pipeline/test/test_base_scraper.py
"""Tests for the BaseScraper class."""

import json

import pytest

from common.base_scraper import BaseScraper, ScraperError
from common import config


class DummyScraperMock(BaseScraper):
    """Mock scraper for testing with fake data."""

    def scrape_data(self, start_date: str, end_date: str):
        """Mock scraping method that returns fake data."""
        return [
            {"fecha": start_date, "animal": "DELFIN", "numero": "0"},
            {"fecha": end_date, "animal": "LEON", "numero": "05"},
        ]

    def process_data(self, raw_data):
        """Mock processing method that transforms raw data."""
        return [
            {
                "fecha": d["fecha"],
                "numero": d["numero"],
                "animal": d["animal"],
                "fuente": "fake-url"
            }
            for d in raw_data
        ]

    def save_data(self, processed_data, output_format="json"):
        """Mock save method that writes to JSON file."""
        output_file = config.OUTPUTS_DIR / f"{self.name}_data.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
        return output_file


class DummyScraperReal(BaseScraper):
    """Real scraper for testing with actual URL requests."""

    def scrape_data(self, start_date: str, end_date: str):
        """Real scraping method that simulates HTTP request."""
        # Simulate HTTP response without actual request
        return [{"html_content": f"<html>Mock content for {start_date} to {end_date}</html>", "url": f"https://example.com/{start_date}/{end_date}"}]

    def process_data(self, raw_data):
        """Real processing method that extracts data from HTML."""
        return [{"html_length": len(raw_data[0]["html_content"])}]

    def save_data(self, processed_data, output_format="json"):
        """Real save method that writes processed data to JSON."""
        output_file = config.OUTPUTS_DIR / f"{self.name}_html.json"
        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(processed_data, f, ensure_ascii=False, indent=2)
        return output_file


def test_dummy_scraper_mock():
    """Test mock scraper with fake data."""

    scraper = DummyScraperMock(name="dummy_mock", url="http://fake-url.com")
    scraper.run("2025-09-08", "2025-09-14")

    # Verify output file was created
    output_file = config.OUTPUTS_DIR / "dummy_mock_data.json"
    assert output_file.exists()
    assert output_file.name == "dummy_mock_data.json"

    # Verify data content
    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert len(data) == 2
    assert data[0]["animal"] == "DELFIN"
    assert data[1]["animal"] == "LEON"
    assert all("fuente" in item for item in data)


def test_dummy_scraper_real():
    """Test real scraper with simulated data."""

    scraper = DummyScraperReal(name="dummy_real", url="https://example.com")
    scraper.run("2025-09-08", "2025-09-14")

    # Verify output file was created
    output_file = config.OUTPUTS_DIR / "dummy_real_html.json"
    assert output_file.exists()
    assert output_file.name == "dummy_real_html.json"

    # Verify data content
    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)

    assert "html_length" in data[0]
    assert data[0]["html_length"] > 50  # HTML should have minimum size


def test_scraper_error_handling():
    """Test error handling in scraper."""

    class ErrorScraper(BaseScraper):
        """Scraper that raises an error for testing."""

        def scrape_data(self, start_date: str, end_date: str):
            """Raise an error to test error handling."""
            raise ValueError("Test error")

        def process_data(self, raw_data):
            """Dummy process method."""
            return raw_data

        def save_data(self, processed_data, output_format="json"):
            """Dummy save method."""
            return None

    scraper = ErrorScraper(name="error_test", url="http://test.com")

    # Test that error is properly raised
    with pytest.raises(ScraperError, match="Error durante la ejecución"):
        scraper.run("2025-09-08", "2025-09-14")


def test_scraper_data_flow():
    """Test complete data flow through scraper."""

    scraper = DummyScraperMock(name="flow_test", url="http://test.com")

    # Test individual steps
    raw_data = scraper.scrape_data("2025-09-08", "2025-09-14")
    assert len(raw_data) == 2
    assert raw_data[0]["fecha"] == "2025-09-08"

    processed_data = scraper.process_data(raw_data)
    assert len(processed_data) == 2
    assert all("fuente" in item for item in processed_data)

    output_file = scraper.save_data(processed_data)
    assert output_file.exists()

    # Test full run
    scraper.run("2025-09-08", "2025-09-14")
    assert scraper.raw_data == raw_data
    assert scraper.processed_data == processed_data


def test_scraper_empty_data_handling():
    """Test scraper behavior with empty data."""

    class EmptyDataScraper(BaseScraper):
        """Scraper that returns empty data for testing."""

        def scrape_data(self, start_date: str, end_date: str):
            """Return empty data."""
            return []

        def process_data(self, raw_data):
            """Process empty data."""
            return []

        def save_data(self, processed_data, output_format="json"):
            """Handle empty data save."""
            return None

    scraper = EmptyDataScraper(name="empty_test", url="http://test.com")

    # Should not raise error with empty data
    scraper.run("2025-09-08", "2025-09-14")
    assert scraper.raw_data == []
    assert scraper.processed_data == []