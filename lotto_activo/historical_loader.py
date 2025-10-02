# lotto-activo/historical_loader.py
"""
Carga histórica de datos de Lotto Activo - Versión extendida para data-pipeline
Extrae datos semanales del último año y genera un JSON consolidado.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, final
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
    filename=LOGS_DIR / "historical_loader.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8",
)


class HistoricalLoader:
    """Carga histórica de datos de Lotto Activo - Versión extendida para data-pipeline
    Extrae datos semanales del último año y genera un JSON consolidado."""

    def __init__(self, source="LOTERIADEHOY_HISTORICO", output_file=None):
        self.base_url = RESULTADOS_URLS[source]
        self.output_file = Path(output_file) if output_file else None
        # No creamos directorios aquí para evitar efectos secundarios innecesarios
        
    def _get_output_path(self, start_date: str | datetime, end_date: str | datetime, yearly: bool = False) -> Path:
        """Calcula la ruta del archivo de salida basado en las fechas"""
        
        # Normalizamos fechas
        safe_start = self._sanitize_date(start_date)
        safe_end = self._sanitize_date(end_date)

        if self.output_file and not yearly:
            return self.output_file
            
        # Si output_file no fue especificado o es yearly, usamos nombres dinámicos
        filename = (
            "historical_draws_results_data_year.json" 
            if yearly 
            else f"historical_data_{safe_start}_to_{safe_end}.json"
        )
        return Path(DATA_DIR) / filename

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
                current_end.strftime("%d-%m-%Y"),
            )
            print(
                f"Cargando semana: {current_start:%d-%m-%Y} -> {current_end:%d-%m-%Y}"
            )

            weekly_data = self._load_data_for_range(
                current_start.strftime("%Y-%m-%d"), current_end.strftime("%Y-%m-%d")
            )
            all_data.extend(weekly_data)

            current_start += timedelta(days=7)

        # Guardar todo en un único JSON consolidado
        self._save_to_json(all_data, current_start, current_end, yearly=True)
        return all_data
    
    def load_range_draws(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Carga los de datos segun rango de fechas indicado (formato DD-MM-YYYY)"""
        final_date = datetime.strptime(end_date, '%d-%m-%Y')
        current_start = datetime.strptime(start_date, '%d-%m-%Y')

        all_data: List[Dict[str, Any]] = []

        while current_start <= final_date:
            week_end = min(current_start + timedelta(days=6), final_date)
            logging.info(
                "Cargando semana: %s -> %s",
                current_start.strftime("%d-%m-%Y"),
                week_end.strftime("%d-%m-%Y"),
            )
            print(
                f"Cargando semana: {current_start:%d-%m-%Y} -> {week_end:%d-%m-%Y}"
            )

            weekly_data = self._load_data_for_range(
                current_start.strftime("%Y-%m-%d"), week_end.strftime("%Y-%m-%d")
            )
            all_data.extend(weekly_data)

            # Avanzar a la siguiente semana
            current_start = week_end + timedelta(days=1)

        # Guardar todo en un único JSON consolidado
        self._save_to_json(all_data, start_date, end_date, yearly=False)
        return all_data

    def _load_data_for_range(
        self, start_date: str, end_date: str
    ) -> List[Dict[str, Any]]:
        """Carga datos para un rango semanal específico"""
        url = self.base_url.format(start=start_date, end=end_date)
        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            table = soup.find("table", {"id": "table"})

            if not table:
                logging.warning(
                    "No se encontró tabla en el rango %s -> %s", start_date, end_date
                )
                print(f"⚠️ No se encontró tabla en el rango {start_date} -> {end_date}")
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

    def _extract_table_data(
        self, table, start_date: str, end_date: str
    ) -> List[Dict[str, Any]]:
        """Extrae datos de la tabla semanal de LoteriaDeHoy (pivotada por fechas).
           Formato ESTRUCTURADO (para producción).
        """

        data = []

        # Obtener las fechas desde el encabezado (omitimos "Horario")
        headers = [th.get_text(strip=True) for th in table.select("thead th")][1:]

        # Recorrer filas del cuerpo
        for row in table.select("tbody tr"):
            hora = row.find("th").get_text(strip=True)  # Columna de horario
            celdas = row.find_all("td")

            for i, celda in enumerate(celdas):
                fecha = headers[i]  # Fecha asociada a esta columna
                animal = celda.get_text(strip=True).title()  # Normalizar capitalización
                imagen = celda.find("img")["src"] if celda.find("img") else None
                numero = ANIMAL_TO_NUMBER.get(animal.upper())
                color = None
                
                if "class" in celda.attrs:
                    clases = celda["class"]
                    if len(clases) > 0:
                        color = clases[0]  # "rojo" o "negro"
                        
                registro = {
                    "sorteo": {
                        "fecha": fecha,
                        "hora": hora,
                        "animal": animal,
                        "numero": numero,
                        "color": color,
                        "imagen": imagen,
                    },
                    "fuente_scraper": {
                        "url_fuente": "https://loteriadehoy.com/animalito/lottoactivo/historico/",
                        "rango_fechas": {
                            "inicio": start_date,
                            "fin": end_date,
                        },
                        "script": "historical_loader",
                        "procesado_el": datetime.now().isoformat(),
                        "validado": numero is not None,
                    },
                }
                data.append(registro)

        return data
    
    def _extract_table_data_plain(
        self, table, start_date: str, end_date: str
    ) -> List[Dict[str, Any]]:
        """Extrae datos de la tabla semanal de LoteriaDeHoy (pivotada por fechas).
           Formato PLANO (para tests/depuración).
        """

        data = []

        # Obtener las fechas desde el encabezado (omitimos "Horario")
        headers = [th.get_text(strip=True) for th in table.select("thead th")][1:]

        # Recorrer filas del cuerpo
        for row in table.select("tbody tr"):
            hora = row.find("th").get_text(strip=True)  # Columna de horario
            celdas = row.find_all("td")

            for i, celda in enumerate(celdas):
                fecha = headers[i]  # Fecha asociada a esta columna
                animal = celda.get_text(strip=True).title()  # Normalizar capitalización
                numero = ANIMAL_TO_NUMBER.get(animal.upper())

                registro = {
                    "fecha": fecha,
                    "hora": hora,
                    "animal": animal,
                    "numero": numero,
                    "rango": f"{start_date} -> {end_date}",
                    "fuente": "loteriadehoy.com",
                }
                data.append(registro)

        return data

    def _sanitize_date(self, date_value: str | datetime) -> str:
        """Convierte fecha (datetime o string) a formato YYYY-MM-DD seguro para nombres de archivo"""
        if isinstance(date_value, datetime):
            return date_value.strftime("%Y-%m-%d")
        # Si ya es string con hora → recortamos solo la parte de fecha
        return str(date_value).split(" ")[0]

    def _save_to_json(self, data, start_date, end_date, yearly=False):
        """Guarda datos en formato JSON y actualiza self.output_file"""
        filename = self._get_output_path(start_date, end_date, yearly)
        
        # Asegurar que el directorio padre existe
        filename.parent.mkdir(parents=True, exist_ok=True)
        
        try:
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.info("Datos guardados en %s.", filename)
            
            # ✅ CRUCIAL: Actualizamos output_file para reflejar la ruta real
            self.output_file = filename
        except IOError as e:
            logging.error("No se pudo guardar el archivo %s: %s", filename, e)


# Ejecución directa
if __name__ == "__main__":
    loader = HistoricalLoader()
    # historical_data = loader.load_last_year()
    historical_data = loader.load_range_draws('01-09-2024', '30-09-2025')
    print(f"Carga completada. {len(historical_data)} registros obtenidos.")

