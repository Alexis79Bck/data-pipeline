# tests/test_last_draw_result.py

import json
import pytest
from pathlib import Path
from datetime import datetime
from unittest.mock import patch, MagicMock

import lotto_activo.last_draw_result as ldr


@pytest.fixture
def fetcher(tmp_path):
    """Instancia de LastDrawFetcher con directorio temporal para outputs"""
    return ldr.LastDrawFetcher(output_dir=tmp_path)


def test_sanitize_date_str_and_datetime(fetcher):
    assert fetcher._sanitize_date("2025-09-27") == "2025-09-27"
    dt = datetime(2025, 9, 27)
    assert fetcher._sanitize_date(dt) == dt.strftime(ldr.DATE_FORMAT)


def test_parse_block_valid(fetcher):
    html = """
    <div class="col-sm-6">
        <h4 class="rojo mt-3">12 Tigre</h4>
        <h5>10:00 AM</h5>
        <div class="circle"><img src="tigre.png" /></div>
    </div>
    """
    from bs4 import BeautifulSoup
    soup = BeautifulSoup(html, "html.parser")
    block = soup.find("div", class_="col-sm-6")

    result = fetcher._parse_block(block, "2025-09-27")
    assert result["sorteo"]["numero"] == "12"
    assert result["sorteo"]["animal"] == "Tigre"
    assert result["sorteo"]["color"] == "rojo"
    assert result["sorteo"]["imagen"] == "tigre.png"


def test_parse_block_invalid(fetcher):
    from bs4 import BeautifulSoup
    html = """<div class="col-sm-6"><h4>SinNumero</h4></div>"""
    soup = BeautifulSoup(html, "html.parser")
    block = soup.find("div", class_="col-sm-6")

    result = fetcher._parse_block(block, "2025-09-27")
    assert result is None


def test_append_to_json_adds_and_avoids_duplicates(fetcher, tmp_path):
    result = {
        "sorteo": {"hora": "10:00 AM", "numero": "12", "animal": "Tigre", "color": "rojo", "fecha": "2025-09-27", "imagen": "tigre.png"},
        "fuente_scraper": {"url_fuente": "http://fake", "fecha": "2025-09-27", "script": "test", "procesado_el": datetime.now().isoformat()},
        "validado": True,
    }
    safe_date = "2025-09-27"

    fetcher._append_to_json(result, safe_date)
    output_file = tmp_path / f"last_results_{safe_date}.json"

    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert len(data) == 1

    # Intentar añadir duplicado
    fetcher._append_to_json(result, safe_date)
    with open(output_file, "r", encoding="utf-8") as f:
        data = json.load(f)
    assert len(data) == 1  # sigue siendo uno


@patch("lotto_activo.last_draw_result.requests.get")
def test_fetch_last_result_success(mock_get, fetcher):
    html = """
    <div class="col-sm-6">
        <h4 class="verde mt-3">05 Leon</h4>
        <h5>11:00 AM</h5>
        <div class="circle"><img src="leon.png" /></div>
    </div>
    """
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = html
    mock_get.return_value = mock_response

    result = fetcher.fetch_last_result("2025-09-27")
    assert result is not None
    assert result["sorteo"]["numero"] == "05"
    assert result["sorteo"]["animal"] == "Leon"


@patch("lotto_activo.last_draw_result.requests.get")
def test_fetch_last_result_no_blocks(mock_get, fetcher):
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.text = "<html></html>"
    mock_get.return_value = mock_response

    result = fetcher.fetch_last_result("2025-09-27")
    assert result is None


@patch("lotto_activo.last_draw_result.requests.get")
def test_fetch_last_result_http_error(mock_get, fetcher):
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = Exception("HTTP 500")
    mock_get.return_value = mock_response

    result = fetcher.fetch_last_result("2025-09-27")
    assert result is None
