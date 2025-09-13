# data-pipeline/lotto-activo/scraper.py
"""Lotto Activo scraper implementation with robust error handling and data processing."""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional
# from urllib.parse import urljoin, urlparse  # Unused for now

import requests
from bs4 import BeautifulSoup

from common.base_scraper import BaseScraper, ScrapingError, ProcessingError, SavingError
from common import config
from common.utils import (
    clean_data,
    parse_spanish_date,
    convert_time_12h_to_24h,
    validate_date_range,
    ValidationError,
)


class LottoActivoScraper(BaseScraper):
    """
    Scraper concreto para la lotería Lotto Activo.
    
    Hereda de BaseScraper y implementa la extracción, procesamiento
    y guardado de datos específicos para Lotto Activo con manejo
    robusto de errores y validaciones.
    """

    def __init__(
        self,
        name: str = "lotto-activo",
        url: str = "https://loteriadehoy.com/animalito/lottoactivo/historico/{start}/{end}/",
        max_retries: int = 3,
        retry_delay: float = 2.0,
        timeout: int = 30,
        max_data_size_mb: float = 50.0
    ):
        """
        Inicializa el scraper de Lotto Activo.
        
        Args:
            name: Nombre del scraper
            url: URL base con placeholders {start} y {end}
            max_retries: Número máximo de reintentos
            retry_delay: Delay entre reintentos en segundos
            timeout: Timeout para requests en segundos
            max_data_size_mb: Tamaño máximo de datos en MB
        """
        super().__init__(
            name=name,
            url=url,
            max_retries=max_retries,
            retry_delay=retry_delay,
            timeout=timeout,
            max_data_size_mb=max_data_size_mb
        )
        
        # Configuración específica de Lotto Activo
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'es-ES,es;q=0.9,en;q=0.8',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
        })

    def scrape_data(self, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Extrae los resultados de la página web de Lotto Activo.
        
        Args:
            start_date: Fecha de inicio (YYYY-MM-DD)
            end_date: Fecha de fin (YYYY-MM-DD)
            
        Returns:
            Lista de resultados crudos
            
        Raises:
            ScrapingError: Si hay error durante el scraping
        """
        try:
            # Validar fechas
            if not validate_date_range(start_date, end_date):
                raise ValidationError(f"Rango de fechas inválido: {start_date} → {end_date}")
            
            # Construir URL
            url = self.url.format(start=start_date, end=end_date)
            self.logger.info(f"🌐 Solicitando datos desde: {url}")
            
            # Realizar request con timeout
            response = self.session.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parsear HTML
            soup = BeautifulSoup(response.text, "html.parser")
            
            # Extraer datos de la tabla
            results = self._extract_table_data(soup, start_date, end_date)
            
            if not results:
                self.logger.warning("📭 No se encontraron datos en el rango especificado")
                return []
            
            self.logger.info(f"📥 {len(results)} registros extraídos exitosamente")
            return results
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Error de red al consultar {url}: {str(e)}"
            self.logger.error(error_msg)
            raise ScrapingError(error_msg) from e
        except Exception as e:
            error_msg = f"Error inesperado durante el scraping: {str(e)}"
            self.logger.error(error_msg)
            raise ScrapingError(error_msg) from e

    def _extract_table_data(self, soup: BeautifulSoup, start_date: str, end_date: str) -> List[Dict[str, Any]]:
        """
        Extrae datos de la tabla HTML.
        
        Args:
            soup: Objeto BeautifulSoup parseado
            start_date: Fecha de inicio
            end_date: Fecha de fin
            
        Returns:
            Lista de datos extraídos
        """
        results = []
        
        # Buscar tabla de resultados (ajustar selectores según la página real)
        table_selectors = [
            "table tbody tr",
            ".results-table tbody tr",
            ".lotto-table tbody tr",
            "table tr",
            ".result-row"
        ]
        
        rows = []
        for selector in table_selectors:
            rows = soup.select(selector)
            if rows:
                self.logger.info(f"📋 Tabla encontrada con selector: {selector}")
                break
        
        if not rows:
            self.logger.warning("⚠ No se encontró tabla de resultados")
            return []
        
        for i, row in enumerate(rows):
            try:
                # Extraer celdas
                cells = row.find_all(["td", "th"])
                if len(cells) < 3:
                    continue
                
                # Extraer datos de las celdas
                row_data = self._extract_row_data(cells, i)
                if row_data:
                    results.append(row_data)
                    
            except Exception as e:
                self.logger.warning(f"⚠ Error procesando fila {i}: {str(e)}")
                continue
        
        return results

    def _extract_row_data(self, cells: List, row_index: int) -> Optional[Dict[str, Any]]:
        """
        Extrae datos de una fila de la tabla.
        
        Args:
            cells: Lista de celdas de la fila
            row_index: Índice de la fila
            
        Returns:
            Diccionario con los datos extraídos o None si no es válido
        """
        try:
            # Extraer texto de las celdas
            cell_texts = [cell.get_text(strip=True) for cell in cells]
            
            # Filtrar celdas vacías
            cell_texts = [text for text in cell_texts if text]
            
            if len(cell_texts) < 3:
                return None
            
            # Mapear datos según posición esperada
            # Asumiendo: fecha, número, animal, hora (opcional)
            fecha_raw = cell_texts[0]
            numero_raw = cell_texts[1]
            animal_raw = cell_texts[2]
            hora_raw = cell_texts[3] if len(cell_texts) > 3 else None
            
            # Procesar fecha
            fecha = parse_spanish_date(fecha_raw)
            if not fecha:
                self.logger.warning(f"⚠ Fecha inválida en fila {row_index}: {fecha_raw}")
                return None
            
            # Procesar número
            numero = self._clean_number(numero_raw)
            if not numero:
                self.logger.warning(f"⚠ Número inválido en fila {row_index}: {numero_raw}")
                return None
            
            # Procesar animal
            animal = self._clean_animal(animal_raw)
            if not animal:
                self.logger.warning(f"⚠ Animal inválido en fila {row_index}: {animal_raw}")
                return None
            
            # Procesar hora si existe
            hora = None
            if hora_raw:
                hora = convert_time_12h_to_24h(hora_raw)
            
            return {
                "fecha": fecha,
                "numero": numero,
                "animal": animal,
                "hora": hora,
                "fecha_raw": fecha_raw,
                "numero_raw": numero_raw,
                "animal_raw": animal_raw,
                "hora_raw": hora_raw,
                "fila": row_index + 1
            }
            
        except Exception as e:
            self.logger.warning(f"⚠ Error extrayendo datos de fila {row_index}: {str(e)}")
            return None

    def _clean_number(self, numero_raw: str) -> Optional[str]:
        """Limpia y valida el número extraído."""
        if not numero_raw:
            return None
        
        # Remover caracteres no numéricos excepto 0
        numero = re.sub(r'[^\d]', '', numero_raw)
        
        # Validar que sea un número válido (0-36)
        try:
            num_int = int(numero)
            if 0 <= num_int <= 36:
                return f"{num_int:02d}"  # Formato con ceros a la izquierda
        except ValueError:
            pass
        
        return None

    def _clean_animal(self, animal_raw: str) -> Optional[str]:
        """Limpia y valida el animal extraído."""
        if not animal_raw:
            return None
        
        # Limpiar texto
        animal = animal_raw.strip().upper()
        
        # Validar que esté en el mapeo de animales
        if animal in config.ANIMAL_TO_NUMBER:
            return animal
        
        # Buscar coincidencias parciales
        for valid_animal in config.ANIMAL_TO_NUMBER.keys():
            if animal in valid_animal or valid_animal in animal:
                return valid_animal
        
        return None

    def process_data(self, raw_data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Procesa y normaliza los datos extraídos.
        
        Args:
            raw_data: Datos crudos extraídos
            
        Returns:
            Lista de datos procesados
            
        Raises:
            ProcessingError: Si hay error durante el procesamiento
        """
        try:
            if not raw_data:
                return []
            
            processed = []
            valid_count = 0
            invalid_count = 0
            
            for item in raw_data:
                try:
                    processed_item = self._process_single_item(item)
                    if processed_item:
                        processed.append(processed_item)
                        valid_count += 1
                    else:
                        invalid_count += 1
                except Exception as e:
                    self.logger.warning(f"⚠ Error procesando item: {str(e)}")
                    invalid_count += 1
                    continue
            
            # Limpiar datos
            cleaned_data = clean_data(processed)
            
            self.logger.info(f"✅ Procesados: {valid_count} válidos, {invalid_count} inválidos")
            return cleaned_data
            
        except Exception as e:
            error_msg = f"Error durante el procesamiento: {str(e)}"
            self.logger.error(error_msg)
            raise ProcessingError(error_msg) from e

    def _process_single_item(self, item: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Procesa un solo item de datos."""
        try:
            # Validar campos requeridos
            if not all(key in item for key in ["fecha", "numero", "animal"]):
                return None
            
            # Obtener mapeo de animal a número
            numero_map = config.ANIMAL_TO_NUMBER.get(item["animal"], item["numero"])
            
            # Crear item procesado
            processed_item = {
                "fecha": item["fecha"],
                "numero": item["numero"],
                "animal": item["animal"],
                "numero_map": numero_map,
                "fuente": "lotto-activo",
                "scraper": self.name,
                "procesado_en": datetime.now().isoformat(),
                "fila": item.get("fila", 0)
            }
            
            # Agregar hora si existe
            if item.get("hora"):
                processed_item["hora"] = item["hora"]
            
            # Agregar metadatos de validación
            processed_item["validado"] = self._validate_item(processed_item)
            
            return processed_item
            
        except Exception as e:
            self.logger.warning(f"⚠ Error procesando item individual: {str(e)}")
            return None

    def _validate_item(self, item: Dict[str, Any]) -> bool:
        """Valida un item procesado."""
        try:
            # Validar fecha
            datetime.strptime(item["fecha"], "%Y-%m-%d")
            
            # Validar número
            numero = int(item["numero"])
            if not (0 <= numero <= 36):
                return False
            
            # Validar animal
            if item["animal"] not in config.ANIMAL_TO_NUMBER:
                return False
            
            return True
        except (ValueError, KeyError):
            return False

    def save_data(self, processed_data: List[Dict[str, Any]], output_format: str = "json") -> Path:
        """
        Guarda los datos procesados en formato JSON.
        
        Args:
            processed_data: Datos procesados
            output_format: Formato de salida (solo JSON por ahora)
            
        Returns:
            Path del archivo guardado
            
        Raises:
            SavingError: Si hay error durante el guardado
        """
        try:
            if not processed_data:
                self.logger.warning("⚠ No hay datos procesados para guardar")
                return None
            
            # Crear directorio de salida
            output_dir = config.OUTPUTS_DIR / self.name
            output_dir.mkdir(parents=True, exist_ok=True)
            
            # Generar nombre de archivo con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = output_dir / f"lotto_activo_{timestamp}.json"
            
            # Guardar datos
            with open(output_file, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, indent=2, ensure_ascii=False)
            
            # También guardar en data/ para consumo de API
            data_dir = config.DATA_DIR / self.name
            data_dir.mkdir(parents=True, exist_ok=True)
            data_file = data_dir / f"lotto_activo_{timestamp}.json"
            
            with open(data_file, "w", encoding="utf-8") as f:
                json.dump(processed_data, f, indent=2, ensure_ascii=False)
            
            self.logger.info(f"💾 Datos guardados en: {output_file}")
            self.logger.info(f"💾 Datos para API en: {data_file}")
            
            return output_file
            
        except Exception as e:
            error_msg = f"Error guardando datos: {str(e)}"
            self.logger.error(error_msg)
            raise SavingError(error_msg) from e

    def get_latest_data(self, days: int = 7) -> List[Dict[str, Any]]:
        """
        Obtiene los datos más recientes.
        
        Args:
            days: Número de días hacia atrás
            
        Returns:
            Lista de datos más recientes
        """
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
        
        return self.run(start_date, end_date)

    def close(self):
        """Cierra la sesión de requests."""
        if hasattr(self, 'session'):
            self.session.close()