import pytest
from common.base_scraper import BaseScraper

class DummyScraper(BaseScraper):
    def scrape_data(self, start_date, end_date):
        return [{"animal": "05", "date": start_date, "time": "08:00 AM"}]

    def process_data(self):
        return [{"animal": "LEON", "date": "2025-09-08", "time": "08:00:00"}]

    def save_data(self, output_format="json"):
        return True

def test_dummy_scraper_run(tmp_path, monkeypatch):
    # Sobrescribimos la ruta de logs temporalmente
    monkeypatch.setattr("data_pipeline.common.base_scraper.LOGS_DIR", tmp_path)

    scraper = DummyScraper(name="dummy", url="http://fake-url")
    scraper.run("2025-09-08", "2025-09-08")

    assert scraper.scraped_data[0]["animal"] == "LEON"
