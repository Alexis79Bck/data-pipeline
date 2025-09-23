# data-pipeline/common/base_scraper.py
"""Base scraper class with robust error handling and retry logic."""

import time
from abc import ABC, abstractmethod
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional
from urllib.parse import urlparse

import logging

from .config import OUTPUTS_DIR, LOGS_DIR
from .utils import (
    clean_data,
    get_file_size_mb,
    setup_logger,
    validate_date_range,
    ValidationError,
)


class ScraperError(Exception):
    """Excepción base para errores del scraper."""
    pass


class ScrapingError(ScraperError):
    """Excepción para errores durante el scraping."""
    pass


class ProcessingError(ScraperError):
    """Excepción para errores durante el procesamiento."""
    pass


class SavingError(ScraperError):
    """Excepción para errores durante el guardado."""
    pass


class BaseScraper(ABC):
    """
    Clase abstracta base para todos los scrapers de loterías de animalitos.
    
    Define un flujo de trabajo estándar en 3 fases: scrape → process → save.
    Incluye manejo robusto de errores, retry logic, validaciones y métricas.
    """

    def __init__(
        self, 
        name: str, 
        url: str,
        max_retries: int = 3,
        retry_delay: float = 1.0,
        timeout: int = 30,
        max_data_size_mb: float = 100.0
    ):
        """
        Inicializa el scraper base.
        
        Args:
            name: Nombre único del scraper
            url: URL base para el scraping
            max_retries: Número máximo de reintentos
            retry_delay: Delay entre reintentos en segundos
            timeout: Timeout para requests en segundos
            max_data_size_mb: Tamaño máximo de datos en MB
        """
        self.name = name
        self.url = url
        self.max_retries = max_retries
        self.retry_delay = retry_delay
        self.timeout = timeout
        self.max_data_size_mb = max_data_size_mb
        
        # Datos del scraper
        self.raw_data: List[Dict[str, Any]] = []
        self.processed_data: List[Dict[str, Any]] = []
        
        # Métricas
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
        self.total_records: int = 0
        self.successful_records: int = 0
        self.failed_records: int = 0
        
        # Configurar logger
        self.logger = self._setup_scraper_logger()
        
        # Validar configuración
        self._validate_configuration()

    def _setup_scraper_logger(self) -> logging.Logger:
        """Configura un logger específico para este scraper."""
        log_file = LOGS_DIR / f"{self.name}.log"
        return setup_logger(self.name, log_file)

    def _validate_configuration(self) -> None:
        """Valida la configuración del scraper."""
        if not self.name or not isinstance(self.name, str):
            raise ValidationError("El nombre del scraper debe ser una cadena no vacía")
        
        if not self.url or not isinstance(self.url, str):
            raise ValidationError("La URL debe ser una cadena no vacía")
        
        # Validar URL
        try:
            parsed_url = urlparse(self.url)
            if not parsed_url.scheme or not parsed_url.netloc:
                raise ValidationError(f"URL inválida: {self.url}")
        except Exception as e:
            raise ValidationError(f"Error al validar URL: {str(e)}")
        
        if self.max_retries < 0:
            raise ValidationError("max_retries debe ser >= 0")
        
        if self.retry_delay < 0:
            raise ValidationError("retry_delay debe ser >= 0")
        
        if self.timeout <= 0:
            raise ValidationError("timeout debe ser > 0")
        
        if self.max_data_size_mb <= 0:
            raise ValidationError("max_data_size_mb debe ser > 0")

    # ------------------------
    # Métodos abstractos
    # ------------------------
    @abstractmethod
    def scrape_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Extrae los datos crudos de la página web.
        
        Args:
            start_date: Fecha de inicio (YYYY-MM-DD)
            end_date: Fecha de fin (YYYY-MM-DD)
            
        Returns:
            Lista de resultados crudos
            
        Raises:
            ScrapingError: Si hay error durante el scraping
        """
        pass

    @abstractmethod
    def process_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Procesa y limpia los datos crudos.
        
        Args:
            raw_data: Datos sin procesar
            
        Returns:
            Lista de resultados procesados
            
        Raises:
            ProcessingError: Si hay error durante el procesamiento
        """
        pass

    @abstractmethod
    def save_data(self, processed_data: List[Dict[str, Any]], output_format: str = "json") -> Path:
        """
        Guarda los datos procesados en el formato deseado.
        
        Args:
            processed_data: Lista procesada y validada
            output_format: Formato de salida ("json", "csv", etc.)
            
        Returns:
            Path del archivo guardado
            
        Raises:
            SavingError: Si hay error durante el guardado
        """
        pass

    # ------------------------
    # Flujo principal
    # ------------------------
    def run(self, start_date: str, end_date: str) -> Dict[str, Any]:
        """
        Orquesta el flujo completo del scraper con manejo robusto de errores.
        
        Args:
            start_date: Fecha de inicio (YYYY-MM-DD)
            end_date: Fecha de fin (YYYY-MM-DD)
            
        Returns:
            Diccionario con métricas y resultados del scraping
            
        Raises:
            ScraperError: Si hay error crítico durante la ejecución
        """
        self.start_time = datetime.now()
        self.logger.info(f"🚀 Iniciando scraping para {self.name}")
        self.logger.info(f"📅 Rango de fechas: {start_date} → {end_date}")
        
        try:
            # Validar fechas
            if not validate_date_range(start_date, end_date):
                raise ValidationError(f"Rango de fechas inválido: {start_date} → {end_date}")
            
            # Ejecutar pasos con retry logic
            raw_data = self._scrape_step_with_retry(start_date, end_date)
            processed_data = self._process_step_with_retry(raw_data)
            output_file = self._save_step_with_retry(processed_data)
            
            # Calcular métricas finales
            self.end_time = datetime.now()
            metrics = self._calculate_metrics(output_file)
            
            self.logger.info(f"🏁 Flujo completado con éxito para {self.name}")
            self.logger.info(f"📊 Métricas: {metrics}")
            
            return metrics
            
        except Exception as e:
            self.end_time = datetime.now()
            error_msg = f"Error durante la ejecución: {str(e)}"
            self.logger.error(f"💥 {error_msg}", exc_info=True)
            raise ScraperError(error_msg) from e

    # ------------------------
    # Pasos con retry logic
    # ------------------------
    def _scrape_step_with_retry(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """Paso 1: Scraping con retry logic."""
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.info(f"📥 Intento {attempt + 1} de scraping")
                
                data = self.scrape_data(start_date, end_date)
                
                if not data:
                    self.logger.warning("📭 No se encontraron datos en el rango especificado")
                    return []
                
                # Validar tamaño de datos
                data_size = len(str(data)) / (1024 * 1024)  # Aproximación en MB
                if data_size > self.max_data_size_mb:
                    raise ScrapingError(
                        f"Datos demasiado grandes: {data_size:.2f}MB > {self.max_data_size_mb}MB"
                    )
                
                self.raw_data = data
                self.total_records = len(data)
                self.logger.info(f"✅ {len(data)} registros extraídos exitosamente")
                return data
                
            except Exception as e:
                last_error = e
                self.logger.warning(f"⚠ Intento {attempt + 1} falló: {str(e)}")
                
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)  # Backoff exponencial
                    self.logger.info(f"⏳ Reintentando en {delay:.1f} segundos...")
                    time.sleep(delay)
                else:
                    self.logger.error("❌ Todos los intentos de scraping fallaron")
        
        raise ScrapingError(f"Scraping falló después de {self.max_retries + 1} intentos") from last_error

    def _process_step_with_retry(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Paso 2: Procesamiento con retry logic."""
        if not raw_data:
            return []
        
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.info(f"🔄 Intento {attempt + 1} de procesamiento")
                
                processed = self.process_data(raw_data)
                
                if not processed:
                    self.logger.warning("⚠ No se pudieron procesar los datos")
                    return []
                
                # Limpiar datos
                cleaned_data = clean_data(processed)
                
                self.processed_data = cleaned_data
                self.successful_records = len(cleaned_data)
                self.failed_records = len(raw_data) - len(cleaned_data)
                
                self.logger.info(f"✅ {len(cleaned_data)} registros procesados exitosamente")
                return cleaned_data
                
            except Exception as e:
                last_error = e
                self.logger.warning(f"⚠ Intento {attempt + 1} de procesamiento falló: {str(e)}")
                
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)
                    self.logger.info(f"⏳ Reintentando en {delay:.1f} segundos...")
                    time.sleep(delay)
                else:
                    self.logger.error("❌ Todos los intentos de procesamiento fallaron")
        
        raise ProcessingError(f"Procesamiento falló después de {self.max_retries + 1} intentos") from last_error

    def _save_step_with_retry(self, processed_data: List[Dict[str, Any]]) -> Path:
        """Paso 3: Guardado con retry logic."""
        if not processed_data:
            self.logger.warning("⚠ No hay datos procesados para guardar")
            return None
        
        last_error = None
        
        for attempt in range(self.max_retries + 1):
            try:
                self.logger.info(f"💾 Intento {attempt + 1} de guardado")
                
                output_file = self.save_data(processed_data, output_format="json")
                
                if output_file and output_file.exists():
                    file_size = get_file_size_mb(output_file)
                    self.logger.info(f"✅ Datos guardados en {output_file} ({file_size}MB)")
                    return output_file
                else:
                    raise SavingError("El archivo no se creó correctamente")
                
            except Exception as e:
                last_error = e
                self.logger.warning(f"⚠ Intento {attempt + 1} de guardado falló: {str(e)}")
                
                if attempt < self.max_retries:
                    delay = self.retry_delay * (2 ** attempt)
                    self.logger.info(f"⏳ Reintentando en {delay:.1f} segundos...")
                    time.sleep(delay)
                else:
                    self.logger.error("❌ Todos los intentos de guardado fallaron")
        
        raise SavingError(f"Guardado falló después de {self.max_retries + 1} intentos") from last_error

    # ------------------------
    # Métodos de utilidad
    # ------------------------
    def _calculate_metrics(self, output_file: Optional[Path]) -> Dict[str, Any]:
        """Calcula métricas del scraping."""
        duration = None
        if self.start_time and self.end_time:
            duration = (self.end_time - self.start_time).total_seconds()
        
        metrics = {
            "scraper_name": self.name,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": duration,
            "total_records": self.total_records,
            "successful_records": self.successful_records,
            "failed_records": self.failed_records,
            "success_rate": (
                self.successful_records / self.total_records 
                if self.total_records > 0 else 0
            ),
            "output_file": str(output_file) if output_file else None,
            "output_file_size_mb": get_file_size_mb(output_file) if output_file else 0,
        }
        
        return metrics

    def get_status(self) -> Dict[str, Any]:
        """Obtiene el estado actual del scraper."""
        return {
            "name": self.name,
            "url": self.url,
            "raw_data_count": len(self.raw_data),
            "processed_data_count": len(self.processed_data),
            "is_running": self.start_time is not None and self.end_time is None,
            "last_run": self.end_time.isoformat() if self.end_time else None,
        }

    def reset(self) -> None:
        """Resetea el estado del scraper."""
        self.raw_data = []
        self.processed_data = []
        self.start_time = None
        self.end_time = None
        self.total_records = 0
        self.successful_records = 0
        self.failed_records = 0
        self.logger.info("🔄 Estado del scraper reseteado")

    def validate_data_quality(self, data: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Valida la calidad de los datos.
        
        Args:
            data: Datos a validar
            
        Returns:
            Diccionario con métricas de calidad
        """
        if not data:
            return {"valid": False, "issues": ["No hay datos"]}
        
        issues = []
        total_records = len(data)
        valid_records = 0
        
        for i, record in enumerate(data):
            if not isinstance(record, dict):
                issues.append(f"Registro {i} no es un diccionario")
                continue
            
            if not record:
                issues.append(f"Registro {i} está vacío")
                continue
            
            valid_records += 1
        
        quality_metrics = {
            "valid": len(issues) == 0,
            "total_records": total_records,
            "valid_records": valid_records,
            "invalid_records": total_records - valid_records,
            "quality_score": valid_records / total_records if total_records > 0 else 0,
            "issues": issues[:10],  # Solo los primeros 10 issues
            "total_issues": len(issues)
        }
        
        return quality_metrics