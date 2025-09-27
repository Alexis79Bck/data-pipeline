# lotto-activo/last_draw_result.py
"""
Obtención del último sorteo de Lotto Activo.
Versión standalone para data-pipeline.
"""

import json
import time
import logging
from datetime import datetime, timedelta, time as dtime
from pathlib import Path
from typing import Dict, Any, Union
import requests
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
    filename=LOGS_DIR / "last_draw_result.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    encoding="utf-8",
)


class LastDrawFetcher:
    """Scraping del último sorteo disponible de Lotto Activo"""

    def __init__(self, source="LOTERIADEHOY_HOY", output_dir=OUTPUTS_DIR):
        self.base_url = RESULTADOS_URLS[source]
        self.output_dir = Path(output_dir)

    def fetch_last_result(self, draw_date: Union[str, datetime]) -> Dict[str, Any] | None:
        """Obtiene el último sorteo disponible en la fecha"""
        safe_date = self._sanitize_date(draw_date)
        url = self.base_url.format(date=safe_date)

        try:
            response = requests.get(url, headers=DEFAULT_HEADERS, timeout=30)
            response.raise_for_status()

            soup = BeautifulSoup(response.text, "html.parser")
            blocks = soup.find_all("div", class_="col-sm-6")

            if not blocks:
                logging.warning("⚠️ No se encontraron resultados para %s", safe_date)
                return None

            last_block = blocks[-1]  # 🚩 último sorteo publicado
            result = self._parse_block(last_block, safe_date)

            if result:
                self._append_to_json(result, safe_date)
                return result
            return None

        except Timeout:
            logging.error("Timeout al acceder a %s", url)
        except HTTPError as e:
            logging.error("Error HTTP %s en %s", e.response.status_code, url)
        except RequestException as e:
            logging.error("Error de red en %s : %s", url, e)
        except Exception as e:
            logging.exception("Error inesperado en %s : %s", url, e)
        return None

    # -------------------------
    # Métodos internos
    # -------------------------
    def _sanitize_date(self, draw_date: Union[str, datetime]) -> str:
        if isinstance(draw_date, datetime):
            return draw_date.strftime(DATE_FORMAT)
        return str(draw_date)

    def _parse_block(self, block, safe_date: str) -> Dict[str, Any] | None:
        try:
            title_el = block.find("h4")
            schedule_el = block.find("h5")
            if not title_el or not schedule_el:
                return None

            title = title_el.get_text(strip=True)
            schedule = schedule_el.get_text(strip=True)

            parts = title.split(" ", 1)
            if len(parts) < 2 or not parts[0].isdigit():
                return None

            numero = parts[0]
            animal = parts[1].title()

           # Buscar imagen dentro de <div class="circle">
            img_el = block.find("div", class_="circle").find("img") if block.find("div", class_="circle") else None
            img = img_el["src"] if img_el and img_el.has_attr("src") else None
            
          # Extraer color desde la clase de <h4>
            color = None
            if "class" in title_el.attrs:
                clases = title_el["class"]
                # Filtrar "mt-3" y quedarnos con el color
                color = next((c for c in clases if c not in ["mt-3"]), None)


            return {
                "sorteo": {
                    "fecha": safe_date,
                    "hora": schedule,
                    "animal": animal,
                    "numero": numero,
                    "color": color,
                    "imagen": img,
                },
                "fuente_scraper": {
                    "url_fuente": self.base_url.format(date=safe_date),
                    "fecha": safe_date,
                    "script": "last_draw_result",
                    "procesado_el": datetime.now().isoformat(),
                },
                "validado": numero is not None,
            }
        except Exception as e:
            logging.warning("Error procesando bloque: %s", e)
            return None

    def _append_to_json(self, result: Dict[str, Any], safe_date: str):
        """Añade el último sorteo al archivo JSON del día"""
        output_path = self.output_dir / f"last_results_{safe_date}.json"
        self.output_dir.mkdir(parents=True, exist_ok=True)

        data = []
        if output_path.exists():
            with open(output_path, "r", encoding="utf-8") as f:
                try:
                    data = json.load(f)
                except json.JSONDecodeError:
                    data = []

        # evitar duplicados (por hora + número)
        if not any(
            r["sorteo"]["hora"] == result["sorteo"]["hora"]
            and r["sorteo"]["numero"] == result["sorteo"]["numero"]
            for r in data
        ):
            data.append(result)
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2, ensure_ascii=False)
            logging.info("✅ Resultado añadido a %s", output_path)
        else:
            logging.info("⏩ Sorteo ya existe en %s", output_path)


# -------------------------
# Ejecución standalone
# -------------------------
if __name__ == "__main__":
    fetcher = LastDrawFetcher()
    today = datetime.now().date()

    while True:
        now = datetime.now().time()
        if dtime(8, 0) <= now <= dtime(20, 0):
            result = fetcher.fetch_last_result(today)
            if result:
                print("✅ Último sorteo:", json.dumps(result, indent=2, ensure_ascii=False))
                break
            else:
                print("⏳ Aún no hay sorteo disponible, reintentando en 30s...")
                time.sleep(30)
        else:
            print("🌙 Fuera de horario de sorteos (8:00–20:00). Saliendo...")
            break
