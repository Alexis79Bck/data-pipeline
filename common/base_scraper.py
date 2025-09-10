# data-pipeline/common/base_scraper.py

from abc import ABC, abstractmethod
from pathlib import Path
import logging
from datetime import datetime
from typing import List, Dict, Any
from .utils import setup_logger, save_to_json
from .config import OUTPUTS_DIR, LOGS_DIR


class BaseScraper(ABC):
    """
    Clase abstracta base para todos los scrapers de loterías de animalitos.
    Define un flujo de trabajo estándar en 3 fases: scrape → process → save.
    """

    def __init__(self, name: str, url: str):
        self.name = name
        self.url = url
        self.raw_data: List[Dict[str, Any]] = []
        self.processed_data: List[Dict[str, Any]] = []
        self.logger = self._setup_scraper_logger()

    # ------------------------
    # Configuración de Logger
    # ------------------------
    def _setup_scraper_logger(self) -> logging.Logger:
        """Configura un logger específico para este scraper."""
        log_file = LOGS_DIR / f"{self.name}.log"
        return setup_logger(self.name, log_file)

    # ------------------------
    # Métodos abstractos
    # ------------------------
    @abstractmethod
    def scrape_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Extrae los datos crudos de la página web.
        :param start_date: Fecha de inicio (YYYY-MM-DD)
        :param end_date: Fecha de fin (YYYY-MM-DD)
        :return: Lista de resultados crudos
        """
        pass

    @abstractmethod
    def process_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Procesa y limpia los datos crudos.
        - Mapea animales
        - Formatea fechas
        - Valida resultados
        :param raw_data: Datos sin procesar
        :return: Lista de resultados procesados
        """
        pass

    @abstractmethod
    def save_data(self, processed_data: List[Dict[str, Any]], output_format: str = "json") -> None:
        """
        Guarda los datos procesados en el formato deseado.
        :param processed_data: Lista procesada y validada
        :param output_format: "json" (por ahora)
        """
        pass

    # ------------------------
    # Flujo principal (plantilla)
    # ------------------------
    def run(self, start_date: str, end_date: str) -> None:
        """
        Orquesta el flujo completo del scraper.
        Divide las responsabilidades en pasos privados.
        """
        self.logger.info(f"🚀 Iniciando scraping para {self.name}")
        try:
            raw_data = self._scrape_step(start_date, end_date)
            processed_data = self._process_step(raw_data)
            self._save_step(processed_data)
            self.logger.info(f"🏁 Flujo completado con éxito para {self.name}")
        except Exception as e:
            self.logger.error(f"💥 Error durante la ejecución: {str(e)}", exc_info=True)
            raise

    # ------------------------
    # Pasos privados
    # ------------------------
    def _scrape_step(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Paso 1: Scraping de datos crudos"""
        self.logger.info(f"📅 Fechas: {start_date} → {end_date}")
        self.logger.info(f"🌐 URL base: {self.url.format(start=start_date, end=end_date)}")

        data = self.scrape_data(start_date, end_date)
        if not data:
            self.logger.warning("📭 No se encontraron datos en el rango especificado.")
            return []

        self.raw_data = data
        self.logger.info(f"📥 {len(data)} registros extraídos")
        return data

    def _process_step(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Paso 2: Procesamiento y validación de datos"""
        processed = self.process_data(raw_data)
        if not processed:
            self.logger.error("❌ No se pudieron procesar los datos")
            return []

        self.processed_data = processed
        self.logger.info(f"✅ {len(processed)} registros procesados y validados")
        return processed

    def _save_step(self, processed: List[Dict[str, Any]]) -> None:
        """Paso 3: Guardar resultados en disco"""
        if not processed:
            self.logger.warning("⚠ No hay datos procesados para guardar.")
            return

        self.save_data(processed, output_format="json")
        self.logger.info("💾 Datos guardados correctamente en outputs/")
