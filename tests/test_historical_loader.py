# Testing modulo historical loader
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
import pytest
from lotto_activo.historical_loader import HistoricalLoader

# HTML de ejemplo de tabla para mock
MOCK_HTML = """
<table id="table">
    <tr><th>Fecha</th><th>Hora</th><th>Animal</th></tr>
    <tr><td>2025-09-01</td><td>10:00</td><td>Perro</td></tr>
    <tr><td>2025-09-02</td><td>12:00</td><td>Gato</td></tr>
</table>
"""

@pytest.fixture
def loader():
    """Fixture que devuelve una instancia del HistoricalLoader"""
    return HistoricalLoader()

@patch("lotto_activo.historical_loader.requests.get")
def test_load_data_for_range(mock_get, loader, tmp_path):
    """Test del método _load_data_for_range"""
    
    # Configurar el mock de requests
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = MOCK_HTML
    mock_get.return_value = mock_response

    # Cambiar directorio de salida a tmp_path para no tocar DATA_DIR real
    loader.output_file = tmp_path / "historical_test.json"

    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    data = loader._load_data_for_range(start_date, end_date) # pylint: disable=protected-access en la línea específica

    # Verificar que devuelve una lista de dicts
    assert isinstance(data, list)
    assert all(isinstance(d, dict) for d in data)
    assert len(data) == 2

    # Verificar contenido
    assert data[0]["fecha"] == "2025-09-01"
    assert data[0]["animal"] == "Perro"

    # Verificar que se escribió el archivo JSON
    assert loader.output_file.exists()

@patch("lotto_activo.historical_loader.requests.get")
def test_load_last_year(mock_get, loader, tmp_path):
    """Test del método load_last_year - solo 2 semanas para prueba rápida"""
    # Mock de respuesta
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = MOCK_HTML
    mock_get.return_value = mock_response

    loader.output_file = tmp_path / "historical_year_test.json"

    # Reducir el rango de prueba a 2 semanas
    with patch("lotto_activo.historical_loader.datetime") as mock_datetime:
        now = datetime(2025, 9, 14)
        mock_datetime.now.return_value = now
        mock_datetime.side_effect = datetime

        data = loader.load_last_year()

    assert isinstance(data, list)
    # Cada semana tiene 2 registros en MOCK_HTML
    assert len(data) >= 2
    assert loader.output_file.exists()
