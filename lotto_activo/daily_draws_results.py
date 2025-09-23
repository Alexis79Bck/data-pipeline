# lotto-activo/historical_loader.py
"""
Carga histórica de datos de Lotto Activo - Versión extendida para data-pipeline
Extrae datos semanales del último año y genera un JSON consolidado.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any
import requests
import logging
from requests.exceptions import HTTPError, Timeout, RequestException
from bs4 import BeautifulSoup

# Importaciones internas
from common.config import (
    RESULTADOS_URLS,
    DATA_DIR,
    LOGS_DIR,
    DEFAULT_HEADERS,
    ANIMAL_TO_NUMBER,
)

# Configurar logging básico
logging.basicConfig(
    filename=LOGS_DIR / "daily_draws_results.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8",
)


class DailyDrawsFetcher:
    """Scraping resultados de un día de Lotto Activo"""

    def __init__(self, source="LOTERIADEHOY_DIARIO", output_file=None):
        self.base_url = RESULTADOS_URLS[source]
        self.output_file = Path(output_file) if output_file else None

    def fetch_for_date(self, draw_date: str | datetime) -> List[Dict[str, Any]]:
        """Obtiene los resultados de un día específico"""
        safe_date = self._sanitize_date(draw_date)
        url = self.base_url.format(date=safe_date)

        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"id": "table"})

            if not table:
                logging.warning("No se encontró tabla para la fecha %s", safe_date)
                print(f"⚠️ No se encontró tabla para {safe_date}")
                return []

            data = self._extract_table_data(table, safe_date)
            self._save_to_json(data, safe_date)
            return data

        except Timeout:
            logging.error("Timeout al acceder a %s", url)
        except HTTPError as e:
            logging.error("Error HTTP %s en %s", e.response.status_code, url)
        except RequestException as e:
            logging.error("Error de red en %s : %s", url, e)
        except Exception as e:
            logging.exception("Error inesperado en %s : %s", url, e)
        return []

    def _extract_table_data(self, table, draw_date: str) -> List[Dict[str, Any]]:
        """Extrae resultados diarios"""
        data = []
        hora_filas = table.select("tbody tr")

        for row in hora_filas:
            hora = row.find("th").get_text(strip=True)
            celda = row.find("td")  # Para diario, una sola columna
            animal = celda.get_text(strip=True).title()
            numero = ANIMAL_TO_NUMBER.get(animal.upper())

            registro = {
                "sorteo": {
                    "fecha": draw_date,
                    "hora": hora,
                    "animal": animal,
                    "numero": numero,
                },
                "fuente_scraper": {
                    "url_fuente": self.base_url.format(date=draw_date),
                    "script": "daily_draws_results",
                    "procesado_el": datetime.now().isoformat(),
                    "validado": numero is not None,
                },
            }
            data.append(registro)

        return data

    def _sanitize_date(self, date_value: str | datetime) -> str:
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%d")
        return str(date_value).split(" ")[0]

    def _get_output_path(self, draw_date: str) -> Path:
        safe_date = self._sanitize_date(draw_date)
        if self.output_file:
            return self.output_file
        return Path(DATA_DIR) / f"daily_data_{safe_date}.json"

    def _save_to_json(self, data, draw_date: str):
        filename = self._get_output_path(draw_date)
        filename.parent.mkdir(parents=True, exist_ok=True)
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.info("Datos guardados en %s.", filename)
            self.output_file = filename
        except IOError as e:
            logging.error("No se pudo guardar el archivo %s: %s", filename, e)

if __name__ == "__main__":
    fetcher = DailyDrawsFetcher()
    hoy = datetime.now().strftime("%Y-%m-%d")
    daily_data = fetcher.fetch_for_date(hoy)
    print(f"Resultados {hoy}: {len(daily_data)} sorteos obtenidos.")

