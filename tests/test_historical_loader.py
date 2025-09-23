# Testing modulo historical loader
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from pprint import pprint
import pytest
import json
from lotto_activo.historical_loader import HistoricalLoader

# HTML de ejemplo de tabla para mock
MOCK_HTML = """
<table id="table" class="display semanal text-center w-100 table-semanal">
						<thead>
							<tr>
								<th>Horario</th>
								<th>2025-09-15</th><th>2025-09-16</th><th>2025-09-17</th><th>2025-09-18</th>							</tr>
						</thead>
						<tbody>
						<tr><th>08:00 AM</th><td class="negro"><img class="d-block" src="/dist/animals_img/Alacran_2.webp">Alacran</td><td class="negro"><img class="d-block" src="/dist/animals_img/Zorro_2.webp">Zorro</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Burro_2.webp">Burro</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Ardilla_2.webp">Ardilla</td></tr><tr><th>09:00 AM</th><td class="rojo"><img class="d-block" src="/dist/animals_img/Caiman_2.webp">Caiman</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Chivo_2.webp">Chivo</td><td class="negro"><img class="d-block" src="/dist/animals_img/Alacran_2.webp">Alacran</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Caballo_2.webp">Caballo</td></tr><tr><th>10:00 AM</th><td class="rojo"><img class="d-block" src="/dist/animals_img/Paloma_2.webp">Paloma</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Cebra_2.webp">Cebra</td><td class="negro"><img class="d-block" src="/dist/animals_img/Rana_2.webp">Rana</td><td class="negro"><img class="d-block" src="/dist/animals_img/Alacran_2.webp">Alacran</td></tr><tr><th>11:00 AM</th><td class="negro"><img class="d-block" src="/dist/animals_img/Raton_2.webp">Raton</td><td class="negro"><img class="d-block" src="/dist/animals_img/Lapa_2.webp">Lapa</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Carnero_2.webp">Carnero</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Burro_2.webp">Burro</td></tr><tr><th>12:00 PM</th><td class="negro"><img class="d-block" src="/dist/animals_img/Toro_2.webp">Toro</td><td class="negro"><img class="d-block" src="/dist/animals_img/Alacran_2.webp">Alacran</td><td class="negro"><img class="d-block" src="/dist/animals_img/Raton_2.webp">Raton</td><td class="verde"><img class="d-block" src="/dist/animals_img/Ballena_2.webp">Ballena</td></tr><tr><th>01:00 PM</th><td class="negro"><img class="d-block" src="/dist/animals_img/Zorro_2.webp">Zorro</td><td class="negro"><img class="d-block" src="/dist/animals_img/Toro_2.webp">Toro</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Ardilla_2.webp">Ardilla</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Chivo_2.webp">Chivo</td></tr><tr><th>02:00 PM</th><td class="negro"><img class="d-block" src="/dist/animals_img/Cochino_2.webp">Cochino</td><td class="negro"><img class="d-block" src="/dist/animals_img/Tigre_2.webp">Tigre</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Perico_2.webp">Perico</td><td class="negro"><img class="d-block" src="/dist/animals_img/Jirafa_2.webp">Jirafa</td></tr><tr><th>03:00 PM</th><td class="rojo"><img class="d-block" src="/dist/animals_img/Aguila_2.webp">Aguila</td><td class="negro"><img class="d-block" src="/dist/animals_img/Pavo_2.webp">Pavo</td><td class="negro"><img class="d-block" src="/dist/animals_img/Vaca_2.webp">Vaca</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Leon_2.webp">Leon</td></tr><tr><th>04:00 PM</th><td class="rojo"><img class="d-block" src="/dist/animals_img/Cebra_2.webp">Cebra</td><td class="negro"><img class="d-block" src="/dist/animals_img/Mono_2.webp">Mono</td><td class="negro"><img class="d-block" src="/dist/animals_img/Tigre_2.webp">Tigre</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Perico_2.webp">Perico</td></tr><tr><th>05:00 PM</th><td class="rojo"><img class="d-block" src="/dist/animals_img/Gallo_2.webp">Gallo</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Caballo_2.webp">Caballo</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Chivo_2.webp">Chivo</td><td class="negro"><img class="d-block" src="/dist/animals_img/Zamuro_2.webp">Zamuro</td></tr><tr><th>06:00 PM</th><td class="negro"><img class="d-block" src="/dist/animals_img/Zamuro_2.webp">Zamuro</td><td class="negro"><img class="d-block" src="/dist/animals_img/Rana_2.webp">Rana</td><td class="negro"><img class="d-block" src="/dist/animals_img/Vaca_2.webp">Vaca</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Caiman_2.webp">Caiman</td></tr><tr><th>07:00 PM</th><td class="rojo"><img class="d-block" src="/dist/animals_img/Culebra_2.webp">Culebra</td><td class="negro"><img class="d-block" src="/dist/animals_img/Toro_2.webp">Toro</td><td class="negro"><img class="d-block" src="/dist/animals_img/Zorro_2.webp">Zorro</td><td class="rojo"><img class="d-block" src="/dist/animals_img/Oso_2.webp">Oso</td></tr>						</tbody>
					</table>
"""


@pytest.fixture
def loader(tmp_path):
    """Fixture que devuelve una instancia del HistoricalLoader""" 
    output_file = tmp_path / "historical_test.json"
    return HistoricalLoader(output_file=output_file)


@patch("lotto_activo.historical_loader.requests.get")
def test_load_data_for_range(mock_get, loader):
    """Test del método _load_data_for_range"""

    # Configurar el mock de requests
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = MOCK_HTML
    mock_get.return_value = mock_response

    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y-%m-%d")
    end_date = datetime.now().strftime("%Y-%m-%d")

    data = loader._load_data_for_range(
        start_date, end_date
    )  
    # Verificar que devuelve una lista de dicts
    assert isinstance(data, list)
    assert all(isinstance(d, dict) for d in data)
    assert len(data) == 48  # según cuántos items genere el loader con tu mock
    pprint(loader)
    pprint(loader.output_file)
    
    # Verificar contenido del primer item
    assert data[0]["sorteo"]["fecha"] == "2025-09-15"
    assert data[0]["sorteo"]["animal"] == "Alacran"
    assert data[0]["sorteo"]["hora"] == "08:00 AM"
    
    assert data[1]["sorteo"]["fecha"] == "2025-09-16"
    assert data[1]["sorteo"]["animal"] == "Zorro"
    assert data[1]["sorteo"]["hora"] == "08:00 AM"
    
    assert data[14]["sorteo"]["fecha"] == "2025-09-17"
    assert data[14]["sorteo"]["animal"] == "Carnero"
    assert data[14]["sorteo"]["hora"] == "11:00 AM"

    # Guardar explícitamente para verificar output_file
    loader._save_to_json(data, start_date, end_date)
    
    # ✅ Verificar que se escribió el archivo JSON
    # Ahora loader.output_file apunta a la ruta correcta
    assert loader.output_file.exists()
    
    # ✅ Verificar contenido del archivo (opcional pero recomendado)
    with open(loader.output_file) as f:
        saved_data = json.load(f)
    assert len(saved_data) == len(data)


@patch("lotto_activo.historical_loader.requests.get")
def test_load_last_year(mock_get, loader):
    """Test del método load_last_year - solo 2 semanas para prueba rápida"""
    # Mock de respuesta
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = MOCK_HTML
    mock_get.return_value = mock_response

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
