# data-pipeline/test/test_lotto_activo_scraper.py
"""Tests for the LottoActivoScraper class."""

import json
import tempfile
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

import pytest
from bs4 import BeautifulSoup

from lotto_activo.scraper import LottoActivoScraper
from common.base_scraper import ScrapingError, ProcessingError, SavingError
from common import config


class TestLottoActivoScraper:
    """Test cases for LottoActivoScraper class."""

    def setup_method(self):
        """Set up test fixtures."""
        self.scraper = LottoActivoScraper(
            name="test-lotto-activo",
            url="https://test-url.com/lotto/{start}/{end}/",
            max_retries=1,
            retry_delay=0.1,
            timeout=5
        )

    def teardown_method(self):
        """Clean up after tests."""
        if hasattr(self, 'scraper'):
            self.scraper.close()

    def test_scraper_initialization(self):
        """Test scraper initialization."""
        assert self.scraper.name == "test-lotto-activo"
        assert self.scraper.url == "https://test-url.com/lotto/{start}/{end}/"
        assert self.scraper.max_retries == 1
        assert self.scraper.timeout == 5
        assert hasattr(self.scraper, 'session')

    def test_clean_number_valid(self):
        """Test number cleaning with valid inputs."""
        assert self.scraper._clean_number("5") == "05"
        assert self.scraper._clean_number("15") == "15"
        assert self.scraper._clean_number("0") == "00"
        assert self.scraper._clean_number("36") == "36"

    def test_clean_number_invalid(self):
        """Test number cleaning with invalid inputs."""
        assert self.scraper._clean_number("37") is None
        assert self.scraper._clean_number("-1") is None
        assert self.scraper._clean_number("abc") is None
        assert self.scraper._clean_number("") is None
        assert self.scraper._clean_number(None) is None

    def test_clean_animal_valid(self):
        """Test animal cleaning with valid inputs."""
        assert self.scraper._clean_animal("LEON") == "LEON"
        assert self.scraper._clean_animal("leon") == "LEON"
        assert self.scraper._clean_animal("León") == "LEON"
        assert self.scraper._clean_animal("  LEON  ") == "LEON"

    def test_clean_animal_invalid(self):
        """Test animal cleaning with invalid inputs."""
        assert self.scraper._clean_animal("INVALID") is None
        assert self.scraper._clean_animal("") is None
        assert self.scraper._clean_animal(None) is None

    def test_extract_row_data_valid(self):
        """Test row data extraction with valid data."""
        # Mock BeautifulSoup cells
        mock_cells = [
            Mock(get_text=Mock(return_value="15 de enero de 2025")),
            Mock(get_text=Mock(return_value="5")),
            Mock(get_text=Mock(return_value="LEON")),
            Mock(get_text=Mock(return_value="2:30 PM"))
        ]
        
        with patch('lotto_activo.scraper.parse_spanish_date', return_value="2025-01-15"), \
             patch('lotto_activo.scraper.convert_time_12h_to_24h', return_value="14:30:00"):
            
            result = self.scraper._extract_row_data(mock_cells, 0)
            
            assert result is not None
            assert result["fecha"] == "2025-01-15"
            assert result["numero"] == "05"
            assert result["animal"] == "LEON"
            assert result["hora"] == "14:30:00"

    def test_extract_row_data_invalid(self):
        """Test row data extraction with invalid data."""
        mock_cells = [
            Mock(get_text=Mock(return_value="invalid date")),
            Mock(get_text=Mock(return_value="invalid number")),
            Mock(get_text=Mock(return_value="invalid animal"))
        ]
        
        result = self.scraper._extract_row_data(mock_cells, 0)
        assert result is None

    def test_process_single_item_valid(self):
        """Test processing single item with valid data."""
        item = {
            "fecha": "2025-01-15",
            "numero": "05",
            "animal": "LEON",
            "fila": 1
        }
        
        result = self.scraper._process_single_item(item)
        
        assert result is not None
        assert result["fecha"] == "2025-01-15"
        assert result["numero"] == "05"
        assert result["animal"] == "LEON"
        assert result["numero_map"] == "05"
        assert result["fuente"] == "lotto-activo"
        assert result["scraper"] == "test-lotto-activo"
        assert result["validado"] is True

    def test_process_single_item_invalid(self):
        """Test processing single item with invalid data."""
        item = {
            "fecha": "invalid",
            "numero": "invalid",
            "animal": "invalid"
        }
        
        result = self.scraper._process_single_item(item)
        assert result is None

    def test_validate_item_valid(self):
        """Test item validation with valid data."""
        item = {
            "fecha": "2025-01-15",
            "numero": "05",
            "animal": "LEON"
        }
        
        assert self.scraper._validate_item(item) is True

    def test_validate_item_invalid(self):
        """Test item validation with invalid data."""
        invalid_items = [
            {"fecha": "invalid", "numero": "05", "animal": "LEON"},
            {"fecha": "2025-01-15", "numero": "37", "animal": "LEON"},
            {"fecha": "2025-01-15", "numero": "05", "animal": "INVALID"},
            {"fecha": "2025-01-15", "numero": "05"},  # Missing animal
        ]
        
        for item in invalid_items:
            assert self.scraper._validate_item(item) is False

    @patch('requests.Session.get')
    def test_scrape_data_success(self, mock_get):
        """Test successful data scraping."""
        # Mock HTML response
        html_content = """
        <html>
            <body>
                <table>
                    <tbody>
                        <tr>
                            <td>15 de enero de 2025</td>
                            <td>5</td>
                            <td>LEON</td>
                            <td>2:30 PM</td>
                        </tr>
                        <tr>
                            <td>16 de enero de 2025</td>
                            <td>10</td>
                            <td>TIGRE</td>
                            <td>3:45 PM</td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        """
        
        mock_response = Mock()
        mock_response.text = html_content
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response
        
        with patch('lotto_activo.scraper.parse_spanish_date') as mock_parse_date, \
             patch('lotto_activo.scraper.convert_time_12h_to_24h') as mock_convert_time:
            
            mock_parse_date.side_effect = ["2025-01-15", "2025-01-16"]
            mock_convert_time.side_effect = ["14:30:00", "15:45:00"]
            
            result = self.scraper.scrape_data("2025-01-15", "2025-01-16")
            
            assert len(result) == 2
            assert result[0]["fecha"] == "2025-01-15"
            assert result[0]["numero"] == "05"
            assert result[0]["animal"] == "LEON"

    @patch('requests.Session.get')
    def test_scrape_data_network_error(self, mock_get):
        """Test scraping with network error."""
        mock_get.side_effect = Exception("Network error")
        
        with pytest.raises(ScrapingError, match="Error de red"):
            self.scraper.scrape_data("2025-01-15", "2025-01-16")

    def test_process_data_success(self):
        """Test successful data processing."""
        raw_data = [
            {
                "fecha": "2025-01-15",
                "numero": "05",
                "animal": "LEON",
                "fila": 1
            },
            {
                "fecha": "2025-01-16",
                "numero": "10",
                "animal": "TIGRE",
                "fila": 2
            }
        ]
        
        result = self.scraper.process_data(raw_data)
        
        assert len(result) == 2
        assert all(item["validado"] for item in result)
        assert all(item["fuente"] == "lotto-activo" for item in result)

    def test_process_data_empty(self):
        """Test processing empty data."""
        result = self.scraper.process_data([])
        assert result == []

    def test_save_data_success(self):
        """Test successful data saving."""
        processed_data = [
            {
                "fecha": "2025-01-15",
                "numero": "05",
                "animal": "LEON",
                "numero_map": "05",
                "fuente": "lotto-activo",
                "scraper": "test-lotto-activo",
                "procesado_en": "2025-01-15T14:30:00",
                "fila": 1,
                "validado": True
            }
        ]
        
        with tempfile.TemporaryDirectory() as tmpdir:
            # Mock config paths
            with patch('common.config.OUTPUTS_DIR', Path(tmpdir) / "outputs"), \
                 patch('common.config.DATA_DIR', Path(tmpdir) / "data"):
                
                output_file = self.scraper.save_data(processed_data)
                
                assert output_file.exists()
                assert output_file.name.startswith("lotto_activo_")
                assert output_file.name.endswith(".json")
                
                # Verify file content
                with open(output_file, 'r', encoding='utf-8') as f:
                    saved_data = json.load(f)
                
                assert len(saved_data) == 1
                assert saved_data[0]["fecha"] == "2025-01-15"

    def test_save_data_empty(self):
        """Test saving empty data."""
        with pytest.raises(SavingError, match="No hay datos procesados"):
            self.scraper.save_data([])

    @patch('lotto_activo.scraper.LottoActivoScraper.scrape_data')
    @patch('lotto_activo.scraper.LottoActivoScraper.process_data')
    @patch('lotto_activo.scraper.LottoActivoScraper.save_data')
    def test_run_success(self, mock_save, mock_process, mock_scrape):
        """Test successful complete run."""
        # Mock return values
        mock_scrape.return_value = [{"fecha": "2025-01-15", "numero": "05", "animal": "LEON"}]
        mock_process.return_value = [{"fecha": "2025-01-15", "numero": "05", "animal": "LEON", "validado": True}]
        mock_save.return_value = Path("test_output.json")
        
        result = self.scraper.run("2025-01-15", "2025-01-16")
        
        assert "scraper_name" in result
        assert result["total_records"] == 1
        assert result["successful_records"] == 1
        assert result["success_rate"] == 1.0

    def test_get_latest_data(self):
        """Test getting latest data."""
        with patch.object(self.scraper, 'run') as mock_run:
            mock_run.return_value = {"total_records": 5}
            
            result = self.scraper.get_latest_data(days=7)
            
            assert result == {"total_records": 5}
            mock_run.assert_called_once()

    def test_close(self):
        """Test closing the scraper."""
        # Mock session close method
        self.scraper.session.close = Mock()
        
        self.scraper.close()
        
        self.scraper.session.close.assert_called_once()

    def test_extract_table_data_no_table(self):
        """Test table extraction when no table is found."""
        html_content = "<html><body><p>No table here</p></body></html>"
        soup = BeautifulSoup(html_content, "html.parser")
        
        result = self.scraper._extract_table_data(soup, "2025-01-15", "2025-01-16")
        
        assert result == []

    def test_extract_table_data_with_table(self):
        """Test table extraction with valid table."""
        html_content = """
        <html>
            <body>
                <table>
                    <tbody>
                        <tr>
                            <td>15 de enero de 2025</td>
                            <td>5</td>
                            <td>LEON</td>
                        </tr>
                    </tbody>
                </table>
            </body>
        </html>
        """
        soup = BeautifulSoup(html_content, "html.parser")
        
        with patch('lotto_activo.scraper.parse_spanish_date', return_value="2025-01-15"):
            result = self.scraper._extract_table_data(soup, "2025-01-15", "2025-01-16")
            
            assert len(result) == 1
            assert result[0]["fecha"] == "2025-01-15"
            assert result[0]["numero"] == "05"
            assert result[0]["animal"] == "LEON"


class TestLottoActivoScraperIntegration:
    """Integration tests for LottoActivoScraper."""

    def test_full_workflow_mock(self):
        """Test complete workflow with mocked data."""
        scraper = LottoActivoScraper(
            name="integration-test",
            url="https://test-url.com/lotto/{start}/{end}/",
            max_retries=1,
            retry_delay=0.1
        )
        
        try:
            # Mock the entire scraping process
            with patch.object(scraper, 'scrape_data') as mock_scrape, \
                 patch.object(scraper, 'process_data') as mock_process, \
                 patch.object(scraper, 'save_data') as mock_save:
                
                # Setup mocks
                mock_scrape.return_value = [{"fecha": "2025-01-15", "numero": "05", "animal": "LEON"}]
                mock_process.return_value = [{"fecha": "2025-01-15", "numero": "05", "animal": "LEON", "validado": True}]
                mock_save.return_value = Path("test_output.json")
                
                # Run the scraper
                result = scraper.run("2025-01-15", "2025-01-16")
                
                # Verify calls
                mock_scrape.assert_called_once_with("2025-01-15", "2025-01-16")
                mock_process.assert_called_once()
                mock_save.assert_called_once()
                
                # Verify result
                assert "scraper_name" in result
                assert result["total_records"] == 1
                
        finally:
            scraper.close()

    def test_error_handling_chain(self):
        """Test error handling throughout the chain."""
        scraper = LottoActivoScraper(
            name="error-test",
            url="https://test-url.com/lotto/{start}/{end}/",
            max_retries=1,
            retry_delay=0.1
        )
        
        try:
            # Test scraping error
            with patch.object(scraper, 'scrape_data', side_effect=ScrapingError("Test error")):
                with pytest.raises(ScrapingError):
                    scraper.run("2025-01-15", "2025-01-16")
            
            # Test processing error
            with patch.object(scraper, 'scrape_data', return_value=[{"test": "data"}]), \
                 patch.object(scraper, 'process_data', side_effect=ProcessingError("Test error")):
                with pytest.raises(ProcessingError):
                    scraper.run("2025-01-15", "2025-01-16")
            
            # Test saving error
            with patch.object(scraper, 'scrape_data', return_value=[{"test": "data"}]), \
                 patch.object(scraper, 'process_data', return_value=[{"test": "data"}]), \
                 patch.object(scraper, 'save_data', side_effect=SavingError("Test error")):
                with pytest.raises(SavingError):
                    scraper.run("2025-01-15", "2025-01-16")
                    
        finally:
            scraper.close()
