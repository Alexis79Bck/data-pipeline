# data-pipeline/common/utils.py
"""Utility functions for data processing and file operations."""

import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

# Logger para este módulo
logger = logging.getLogger(__name__)


class ValidationError(Exception):
    """Excepción personalizada para errores de validación."""
    pass


class DataProcessingError(Exception):
    """Excepción personalizada para errores de procesamiento de datos."""
    pass


def setup_logger(name: str, log_file: Path, level: int = logging.INFO) -> logging.Logger:
    """
    Configura un logger por módulo, evitando handlers duplicados.
    
    Args:
        name: Nombre del logger
        log_file: Archivo de log
        level: Nivel de logging
        
    Returns:
        Logger configurado
    """
    logger = logging.getLogger(name)
    logger.setLevel(level)
    
    if not logger.handlers:
        handler = logging.FileHandler(log_file, encoding='utf-8')
        formatter = logging.Formatter(
            '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    
    return logger


def validate_input(data: Any, expected_type: type, field_name: str = "input") -> None:
    """
    Valida que el input sea del tipo esperado.
    
    Args:
        data: Datos a validar
        expected_type: Tipo esperado
        field_name: Nombre del campo para mensajes de error
        
    Raises:
        ValidationError: Si el tipo no es el esperado
    """
    if data is None:
        raise ValidationError(f"{field_name} no puede ser None")
    
    if not isinstance(data, expected_type):
        raise ValidationError(
            f"{field_name} debe ser de tipo {expected_type.__name__}, "
            f"recibido {type(data).__name__}"
        )


def save_to_json(
    data: Any, 
    filepath: Path, 
    create_backup: bool = True,
    validate_data: bool = True
) -> Path:
    """
    Guarda datos en un archivo JSON con validaciones y backup.
    
    Args:
        data: Datos a guardar
        filepath: Ruta del archivo
        create_backup: Si crear backup del archivo existente
        validate_data: Si validar los datos antes de guardar
        
    Returns:
        Path del archivo guardado
        
    Raises:
        ValidationError: Si los datos no son válidos
        DataProcessingError: Si hay error al guardar
    """
    try:
        validate_input(filepath, Path, "filepath")
        
        if validate_data and data is None:
            raise ValidationError("Los datos no pueden ser None")
        
        # Crear backup si el archivo existe
        if create_backup and filepath.exists():
            backup_path = filepath.with_suffix(f"{filepath.suffix}.backup")
            filepath.rename(backup_path)
            logger.info(f"Backup creado: {backup_path}")
        
        # Crear directorio padre si no existe
        filepath.parent.mkdir(parents=True, exist_ok=True)
        
        # Guardar datos
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Datos guardados exitosamente en: {filepath}")
        return filepath
        
    except (OSError, IOError) as e:
        error_msg = f"Error al guardar archivo {filepath}: {str(e)}"
        logger.error(error_msg)
        raise DataProcessingError(error_msg) from e
    except Exception as e:
        error_msg = f"Error inesperado al guardar {filepath}: {str(e)}"
        logger.error(error_msg)
        raise DataProcessingError(error_msg) from e


def load_from_json(filepath: Path, default: Any = None) -> Any:
    """
    Carga datos desde un archivo JSON con manejo de errores robusto.
    
    Args:
        filepath: Ruta del archivo
        default: Valor por defecto si no se puede cargar
        
    Returns:
        Datos cargados o valor por defecto
    """
    try:
        validate_input(filepath, Path, "filepath")
        
        if not filepath.exists():
            logger.warning(f"Archivo no encontrado: {filepath}")
            return default
        
        with open(filepath, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        logger.info(f"Datos cargados exitosamente desde: {filepath}")
        return data
        
    except json.JSONDecodeError as e:
        error_msg = f"Error al decodificar JSON en {filepath}: {str(e)}"
        logger.error(error_msg)
        return default
    except (OSError, IOError) as e:
        error_msg = f"Error al leer archivo {filepath}: {str(e)}"
        logger.error(error_msg)
        return default
    except Exception as e:
        error_msg = f"Error inesperado al cargar {filepath}: {str(e)}"
        logger.error(error_msg)
        return default


def parse_spanish_date(date_str: str) -> Optional[str]:
    """
    Convierte fecha en español a formato ISO.
    
    Args:
        date_str: Fecha en formato español (ej: "6 de septiembre de 2025")
        
    Returns:
        Fecha en formato ISO (YYYY-MM-DD) o None si es inválida
        
    Examples:
        >>> parse_spanish_date("6 de septiembre de 2025")
        '2025-09-06'
        >>> parse_spanish_date("fecha inválida")
        None
    """
    try:
        validate_input(date_str, str, "date_str")
        
        if not date_str.strip():
            logger.warning("Fecha vacía proporcionada")
            return None
        
        months = {
            'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
            'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
            'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
        }
        
        match = re.search(r'(\d{1,2}) de (\w+) de (\d{4})', date_str.lower().strip())
        if not match:
            logger.warning(f"Formato de fecha no reconocido: {date_str}")
            return None
        
        day, month, year = match.groups()
        
        if month not in months:
            logger.warning(f"Mes no válido: {month}")
            return None
        
        # Validar día y año
        day_int = int(day)
        year_int = int(year)
        
        if not (1 <= day_int <= 31):
            logger.warning(f"Día fuera de rango: {day}")
            return None
        
        if not (1900 <= year_int <= 2100):
            logger.warning(f"Año fuera de rango: {year}")
            return None
        
        result = f"{year}-{months[month]}-{day.zfill(2)}"
        logger.debug(f"Fecha convertida: {date_str} -> {result}")
        return result
        
    except (ValueError, AttributeError) as e:
        logger.error(f"Error al procesar fecha '{date_str}': {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado al procesar fecha '{date_str}': {str(e)}")
        return None


def convert_time_12h_to_24h(time_str: str) -> Optional[str]:
    """
    Convierte tiempo de formato 12h a 24h.
    
    Args:
        time_str: Tiempo en formato 12h (ej: "08:00 AM")
        
    Returns:
        Tiempo en formato 24h (HH:MM:SS) o None si es inválido
        
    Examples:
        >>> convert_time_12h_to_24h("08:00 AM")
        '08:00:00'
        >>> convert_time_12h_to_24h("08:00 PM")
        '20:00:00'
    """
    try:
        validate_input(time_str, str, "time_str")
        
        if not time_str.strip():
            logger.warning("Tiempo vacío proporcionado")
            return None
        
        match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)', 
                         time_str.strip(), re.IGNORECASE)
        if not match:
            logger.warning(f"Formato de tiempo no reconocido: {time_str}")
            return None
        
        hour, minute, second, period = match.groups()
        hour = int(hour)
        minute = int(minute)
        second = int(second) if second else 0
        period = period.upper()
        
        # Validar rangos
        if not (1 <= hour <= 12):
            logger.warning(f"Hora fuera de rango: {hour}")
            return None
        
        if not (0 <= minute <= 59):
            logger.warning(f"Minuto fuera de rango: {minute}")
            return None
        
        if not (0 <= second <= 59):
            logger.warning(f"Segundo fuera de rango: {second}")
            return None
        
        # Convertir a 24h
        if period == "PM" and hour != 12:
            hour += 12
        elif period == "AM" and hour == 12:
            hour = 0
        
        result = f"{hour:02d}:{minute:02d}:{second:02d}"
        logger.debug(f"Tiempo convertido: {time_str} -> {result}")
        return result
        
    except (ValueError, AttributeError) as e:
        logger.error(f"Error al procesar tiempo '{time_str}': {str(e)}")
        return None
    except Exception as e:
        logger.error(f"Error inesperado al procesar tiempo '{time_str}': {str(e)}")
        return None


def validate_date_range(start_date: str, end_date: str) -> bool:
    """
    Valida que el rango de fechas sea válido.
    
    Args:
        start_date: Fecha de inicio (YYYY-MM-DD)
        end_date: Fecha de fin (YYYY-MM-DD)
        
    Returns:
        True si el rango es válido, False en caso contrario
    """
    try:
        validate_input(start_date, str, "start_date")
        validate_input(end_date, str, "end_date")
        
        start_dt = datetime.strptime(start_date, "%Y-%m-%d")
        end_dt = datetime.strptime(end_date, "%Y-%m-%d")
        
        if start_dt > end_dt:
            logger.warning(f"Fecha de inicio posterior a fecha de fin: {start_date} > {end_date}")
            return False
        
        # Validar que no sea más de 1 año
        if (end_dt - start_dt).days > 365:
            logger.warning(f"Rango de fechas muy amplio: {(end_dt - start_dt).days} días")
            return False
        
        return True
        
    except ValueError as e:
        logger.error(f"Formato de fecha inválido: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Error inesperado al validar fechas: {str(e)}")
        return False


def clean_data(data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Limpia y valida una lista de diccionarios.
    
    Args:
        data: Lista de diccionarios a limpiar
        
    Returns:
        Lista limpia de diccionarios
    """
    try:
        validate_input(data, list, "data")
        
        cleaned = []
        for i, item in enumerate(data):
            if not isinstance(item, dict):
                logger.warning(f"Item {i} no es un diccionario, omitiendo")
                continue
            
            # Limpiar valores None y strings vacíos
            cleaned_item = {
                k: v for k, v in item.items() 
                if v is not None and (not isinstance(v, str) or v.strip())
            }
            
            if cleaned_item:  # Solo agregar si tiene datos
                cleaned.append(cleaned_item)
        
        logger.info(f"Datos limpiados: {len(data)} -> {len(cleaned)} elementos")
        return cleaned
        
    except Exception as e:
        logger.error(f"Error al limpiar datos: {str(e)}")
        return []


def get_file_size_mb(filepath: Path) -> float:
    """
    Obtiene el tamaño de un archivo en MB.
    
    Args:
        filepath: Ruta del archivo
        
    Returns:
        Tamaño en MB
    """
    try:
        if not filepath.exists():
            return 0.0
        
        size_bytes = filepath.stat().st_size
        size_mb = size_bytes / (1024 * 1024)
        return round(size_mb, 2)
        
    except Exception as e:
        logger.error(f"Error al obtener tamaño de archivo {filepath}: {str(e)}")
        return 0.0