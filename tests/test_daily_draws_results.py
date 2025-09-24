# Testing modulo daily draws results
from unittest.mock import patch, MagicMock
from datetime import datetime, timedelta
from pprint import pprint
import pytest
import json
from lotto_activo.daily_draws_results import DailyDrawsFetcher

# Mock HTML simplificado para pruebas
MOCK_HTML = """
<div class="row text-center js-con">
	<div class="col-sm-6 col-md-4 col-lg-16 mb-5">
		<div class="circle">
			<img src="/dist/animals_img/Mono_2.webp">
		</div>
		<div class="circle-legend">
			<h4 class="mt-3 negro">13 Mono</h4>
			<h5>Lotto Activo 08:00 AM</h5>
		</div>
	</div>
    <div class="col-sm-6 col-md-4 col-lg-16 mb-5">
        <div class="circle">
            <img src="/dist/animals_img/Carnero_2.webp">
        </div>
        <div class="circle-legend">
            <h4 class="mt-3 rojo">1 Carnero</h4>
            <h5>Lotto Activo 09:00 AM</h5>
        </div>
    </div>
    <div class="col-sm-6 col-md-4 col-lg-16 mb-5">
        <div class="circle">
            <img src="/dist/animals_img/Aguila_2.webp">
        </div>
        <div class="circle-legend">
            <h4 class="mt-3 rojo">9 Aguila</h4>
            <h5>Lotto Activo 10:00 AM</h5>
        </div>
    </div>
    <div class="col-sm-6 col-md-4 col-lg-16 mb-5">
        <div class="circle">
            <img src="/dist/animals_img/Caiman_2.webp">
        </div>
        <div class="circle-legend">
            <h4 class="mt-3 rojo">30 Caiman</h4>
            <h5>Lotto Activo 11:00 AM</h5>
        </div>
    </div>
    <div class="col-sm-6 col-md-4 col-lg-16 mb-5">
        <div class="circle">
            <img src="/dist/animals_img/Pavo_2.webp">
        </div>
        <div class="circle-legend">
            <h4 class="mt-3 negro">17 Pavo</h4>
            <h5>Lotto Activo 12:00 PM</h5>
        </div>
    </div>
    <div class="col-sm-6 col-md-4 col-lg-16 mb-5">
		<div class="circle">
			<img src="/dist/animals_img/Cebra_2.webp">
		</div>
		<div class="circle-legend">
			<h4 class="mt-3 rojo">23 Cebra</h4>
			<h5>Lotto Activo 01:00 PM</h5>
		</div>
	</div>
 </div>
						
"""

# -------------------------
# Tests
# -------------------------
@pytest.fixture
def fetcher():
    """Instancia básica del fetcher para pruebas"""
    return DailyDrawsFetcher()


@patch("requests.get")
def test_fetch_for_date_returns_results(mock_get, fetcher, tmp_path):
    """Debe devolver resultados estructurados para un día con HTML válido"""
    # Configurar el mock de requests.get
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = MOCK_HTML
    mock_get.return_value = mock_response

    test_date = datetime(2025, 8, 3).date()
    results = fetcher.fetch_for_date(test_date)

    # Validaciones
    assert isinstance(results, list)
    assert len(results) == 6  # solo hay 6 bloques en el MOCK_HTML

    # Validar estructura del primer resultado
    first = results[0]
    assert "sorteo" in first
    assert "fuente_scraper" in first
    assert "validado" in first

    # Verificar contenido del sorteo
    sorteo = first["sorteo"]
    assert sorteo["fecha"] == "2025-08-03"
    assert sorteo["animal"] in ["Mono", "Carnero"]
    assert sorteo["numero"] in ["13", "1"]
    assert "hora" in sorteo
    assert "imagen" in sorteo

    # Verificar metadatos
    fuente = first["fuente_scraper"]
    assert "url_fuente" in fuente
    assert "procesado_el" in fuente
    assert "script" in fuente
    assert fuente["script"] == "daily_draws_results"
    assert first["validado"] is True


@patch("requests.get")
def test_fetch_for_date_handles_empty_page(mock_get, fetcher):
    """Debe manejar correctamente cuando no hay resultados"""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html><body><div>No hay datos</div></body></html>"
    mock_get.return_value = mock_response

    test_date = datetime(2025, 9, 22).date()
    results = fetcher.fetch_for_date(test_date)

    assert results == []


@patch("requests.get")
def test_fetch_for_date_network_error(mock_get, fetcher):
    """Debe devolver [] si ocurre un error de red"""
    mock_get.side_effect = Exception("Network error")

    test_date = datetime(2025, 9, 22).date()
    results = fetcher.fetch_for_date(test_date)

    assert results == []