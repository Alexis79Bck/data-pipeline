# data-pipeline/common/base_scraper.py
from abc import ABC, abstractmethod
from pathlib import Path
import logging
from datetime import datetime
from .utils import setup_logger, save_to_json
from .config import OUTPUTS_DIR, LOGS_DIR

class BaseScraper(ABC):
    """
    Clase abstracta base para todos los scrapers de loterías de animalitos.
    Define un flujo de trabajo estándar: scrape → process → save.
    """
    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.scraped_data = []
        self.logger = self._setup_scraper_logger()

    def _setup_scraper_logger(self) -> logging.Logger:
        """Configura un logger específico para este scraper."""
        log_file = LOGS_DIR / f"{self.name}.log"
        return setup_logger(self.name, log_file)

    @abstractmethod
    def scrape_data(self, start_date: str, end_date: str) -> list:
        """
        Extrae los datos crudos de la página web.
        :param start_date: Fecha de inicio (formato YYYY-MM-DD)
        :param end_date: Fecha de fin (formato YYYY-MM-DD)
        :return: Lista de datos crudos (diccionarios)
        """
        pass

    @abstractmethod
    def process_data(self) -> list:
        """
        Procesa y limpia los datos crudos.
        Aplica mapeo de animales, formatea fechas, valida datos.
        :return: Lista de datos procesados y validados
        """
        pass

    @abstractmethod
    def save_data(self, output_format: str = "json"):
        """
        Guarda los datos procesados en el formato especificado.
        :param output_format: "json" (por ahora)
        """
        pass

    def run(self, start_date: str, end_date: str):
        """
        Ejecuta el flujo completo del scraper.
        Este es el método principal que se llama desde main.py
        """
        self.logger.info(f"🚀 Iniciando scraping para {self.name}")
        self.logger.info(f"📅 Rango de fechas: {start_date} → {end_date}")
        self.logger.info(f"🌐 URL: {self.url.format(start=start_date, end=end_date)}")

        try:
            # 1. Scrape
            raw_data = self.scrape_data(start_date, end_date)
            if not raw_data:
                self.logger.warning("📭 No se encontraron datos en el rango especificado.")
                return

            self.scraped_data = raw_data
            self.logger.info(f"📥 {len(raw_data)} registros extraídos")

            # 2. Process
            processed_data = self.process_data()
            if not processed_data:
                self.logger.error("❌ No se pudieron procesar los datos")
                return

            self.scraped_data = processed_data
            self.logger.info(f"✅ {len(processed_data)} registros procesados y validados")

            # 3. Save
            self.save_data(output_format="json")
            self.logger.info(f"💾 Datos guardados correctamente para {self.name}")

        except Exception as e:
            self.logger.error(f"💥 Error durante el scraping: {str(e)}", exc_info=True)
            raise