# lotto-activo/daily-draws-results.py
"""
Obtencion de datos de Lotto Activo - Versión extendida para data-pipeline
Extrae datos del dia y por dia determinado y genera un JSON consolidado.
"""

import json
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Union
import requests
import logging
from requests.exceptions import HTTPError, Timeout, RequestException
from bs4 import BeautifulSoup

# Importaciones internas
from common.config import (
    RESULTADOS_URLS,
    DATE_FORMAT,
    OUTPUTS_DIR,
    LOGS_DIR,
    DEFAULT_HEADERS, 
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
        if not draw_date:
            draw_date = datetime.now().date() - timedelta(days=1)
        
        safe_date = self._sanitize_date(draw_date)
        url = self.base_url.format(date=safe_date)

        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            blocks = soup.find_all("div", class_="col-sm-6")

            if not blocks:
                logging.warning(
                    "No se encontraron resultados para la fecha %s", safe_date
                )
                print(f"⚠️ No se encontraron resultados para {safe_date}")
                return []

            data = self._extract_blocks_data(blocks, safe_date)
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

    # -------------------------
    # Métodos internos
    # -------------------------
    def _sanitize_date(self, draw_date: Union[str, datetime]) -> str:
        if isinstance(draw_date, datetime):
            return draw_date.strftime(DATE_FORMAT)
        return str(draw_date)

    def _extract_blocks_data(self, blocks, safe_date: str) -> List[Dict[str, Any]]:
        results = []
        for block in blocks:
            try:
                # 🎯 1. Verificar que sea un bloque válido de resultado
                # (Debe contener un <h4> con número y animal, y un <h5> con hora)
                title_el = block.find("h4")
                schedule_el = block.find("h5")

                if not title_el or not schedule_el:
                    logging.debug("Bloque descartado: no contiene título o horario")
                    continue

                title = title_el.get_text(strip=True)
                schedule = schedule_el.get_text(strip=True)

                # 🎯 2. Parsear el título: formato esperado "34 Venado"
                parts = title.split(" ", 1)
                if len(parts) < 2 or not parts[0].isdigit():
                    logging.debug(f"Bloque descartado: título inválido ({title})")
                    continue

                numero = parts[0]
                animal = parts[1].title()

                # 🎯 3. Imagen (opcional, puede faltar sin romper el parser)
                img_el = block.find("img")
                img = img_el["src"] if img_el and img_el.has_attr("src") else None
                # 🎯 1. Verificar que sea un bloque válido de resultado
                # (Debe contener un <h4> con número y animal, y un <h5> con hora)
                title_el = block.find("h4")
                schedule_el = block.find("h5")

                if not title_el or not schedule_el:
                    logging.debug("Bloque descartado: no contiene título o horario")
                    continue

                title = title_el.get_text(strip=True)
                schedule = schedule_el.get_text(strip=True)

                # 🎯 2. Parsear el título: formato esperado "34 Venado"
                parts = title.split(" ", 1)
                if len(parts) < 2 or not parts[0].isdigit():
                    logging.debug(f"Bloque descartado: título inválido ({title})")
                    continue

                numero = parts[0]
                animal = parts[1].title()

                # 🎯 3. Imagen (opcional, puede faltar sin romper el parser)
                img_el = block.find("img")
                img = img_el["src"] if img_el and img_el.has_attr("src") else None

                results.append(
                    {
                        "sorteo": {
                               "fecha": safe_date,
                                "hora": schedule,
                                "animal": animal,
                                "numero": numero,
                                "imagen": img,
                            },
                        "fuente_scraper": {
                                "url_fuente": self.base_url.format(date=safe_date),
                                "fecha": safe_date,
                                "script": "daily_draws_results",
                                "procesado_el": datetime.now().isoformat(),
                            },
                        "validado": numero is not None,
                    }
                )
            except Exception as e:
                logging.warning("Error procesando un bloque: %s", e)
        return results

    def _save_to_json(self, data: List[Dict[str, Any]], safe_date: str):
        """Guarda resultados diarios en JSON"""
        if not data:
            return

        output_path = (
            self.output_file or Path(OUTPUTS_DIR) / f"daily_results_{safe_date}.json"
        )
        output_path.parent.mkdir(parents=True, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)

        logging.info("Resultados diarios guardados en %s", output_path)


# -------------------------
# Ejecución directa
# -------------------------
if __name__ == "__main__":
    fetcher = DailyDrawsFetcher()
    date_draws = datetime.now().date() - timedelta(days=1)
    results = fetcher.fetch_for_date(date_draws)
    print(f"Se obtuvieron {len(results)} resultados para {date_draws:%Y-%m-%d}")
    if results:
        print(json.dumps(results, indent=2, ensure_ascii=False))
