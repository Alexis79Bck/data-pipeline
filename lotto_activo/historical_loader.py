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
    DEFAULT_HEADERS
)

# Configurar logging básico
logging.basicConfig(
    filename=LOGS_DIR / "historical_loader.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8"
)


class HistoricalLoader:
    '''Carga histórica de datos de Lotto Activo - Versión extendida para data-pipeline
    Extrae datos semanales del último año y genera un JSON consolidado.'''

    def __init__(self, source="LOTERIADEHOY"):
        self.base_url = RESULTADOS_URLS[source]
        self.output_file = Path(DATA_DIR) / "historical_data_last_year.json"
        self.output_file.parent.mkdir(parents=True, exist_ok=True)

    def load_last_year(self) -> List[Dict[str, Any]]:
        """Carga los últimos 12 meses (52 semanas) de datos"""
        today = datetime.now()
        one_year_ago = today - timedelta(days=365)

        all_data: List[Dict[str, Any]] = []

        current_start = one_year_ago
        while current_start < today:
            current_end = min(current_start + timedelta(days=6), today)
            logging.info(
                "Cargando semana: %s -> %s",
                current_start.strftime("%d-%m-%Y"),
                current_end.strftime("%d-%m-%Y")
            )
            print(
                f"Cargando semana: {current_start:%d-%m-%Y} -> {current_end:%d-%m-%Y}")

            weekly_data = self._load_data_for_range(
                current_start.strftime("%Y-%m-%d"),
                current_end.strftime("%Y-%m-%d")
            )
            all_data.extend(weekly_data)

            current_start += timedelta(days=7)

        # Guardar todo en un único JSON consolidado
        self._save_to_json(all_data, current_start, current_end, yearly=True)
        return all_data

    def _load_data_for_range(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Carga datos para un rango semanal específico"""
        url = self.base_url.format(start=start_date, end=end_date)
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'id': 'table'})

            if not table:
                logging.warning(
                    "No se encontró tabla en el rango %s -> %s", start_date, end_date)
                print(
                    f"⚠️ No se encontró tabla en el rango {start_date} -> {end_date}")
                return []

            return self._extract_table_data(table, start_date, end_date)

        except Timeout:
            logging.error("Timeout al acceder a %s", url)
        except HTTPError as e:
            logging.error("Error HTTP %s en  %s", e.response.status_code, url)
        except RequestException as e:
            logging.error("Error de red en %s : %s", url, e)
        except (AttributeError, ValueError) as e:
            logging.error("Error de parseo en %s en %s :", url, e)
        except Exception as e:
            logging.exception("Error inesperado al procesar %s : %s", url, e)
        return []

    def _extract_table_data(self, table, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Extrae datos de la tabla semanal y los normaliza"""
        data = []
        rows = table.find_all("tr")

        for row in rows[1:]:  # saltar encabezado
            cols = [c.get_text(strip=True) for c in row.find_all("td")]
            if not cols:
                continue

            # ⚠️ Adaptar según la estructura real de columnas de la tabla
            registro = {
                "fecha": cols[0],
                "sorteo": cols[1],
                "animal": cols[2],
                "numero": cols[3],
                "rango": f"{start_date} -> {end_date}"
            }
            data.append(registro)

        return data

    def _save_to_json(self, data, start_date, end_date, yearly=False):
        """Guarda datos en formato JSON"""
        if yearly:
            filename = DATA_DIR / \
                f"historical_data_year_{start_date}_to_{end_date}.json"
        else:
            filename = DATA_DIR / \
                f"historical_data_{start_date}_to_{end_date}.json"

        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.info("Datos guardados en %s .", filename)
        except IOError as e:
            logging.error("No se pudo guardar el archivo %s : %s", filename, e)


# Ejecución directa
if __name__ == "__main__":
    loader = HistoricalLoader()
    historical_data = loader.load_last_year()
    print(f"Carga completada. {len(historical_data)} registros obtenidos.")
