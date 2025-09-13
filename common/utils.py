import json
import logging
from datetime import datetime
from pathlib import Path
from .config import LOGS_DIR, OUTPUTS_DIR
import re

def setup_logger(name, log_file, level=logging.INFO):
    """Configura un logger por módulo, evitando handlers duplicados"""
    logger = logging.getLogger(name)
    logger.setLevel(level)
    if not logger.handlers:
        handler = logging.FileHandler(log_file)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger

def save_to_json(data, filepath: Path):
    """Guarda datos en un archivo JSON"""
    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def load_from_json(filepath: Path):
    """Carga datos desde un archivo JSON"""
    if filepath.exists():
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    return None

def parse_spanish_date(date_str: str) -> str:
    """Convierte '6 de septiembre de 2025' → '2025-09-06'"""
    months = {
        'enero': '01', 'febrero': '02', 'marzo': '03', 'abril': '04',
        'mayo': '05', 'junio': '06', 'julio': '07', 'agosto': '08',
        'septiembre': '09', 'octubre': '10', 'noviembre': '11', 'diciembre': '12'
    }
    match = re.search(r'(\d{1,2}) de (\w+) de (\d{4})', date_str.lower())
    if match:
        day, month, year = match.groups()
        if month not in months:
            return None
        return f"{year}-{months[month]}-{day.zfill(2)}"
    return None

def convert_time_12h_to_24h(time_str: str) -> str:
    """Convierte '08:00 AM' → '08:00:00'. Devuelve None si no es válido"""
    match = re.search(r'(\d{1,2}):(\d{2})(?::(\d{2}))?\s*(AM|PM)', time_str, re.IGNORECASE)
    if not match:
        return None
    hour, minute, second, period = match.groups()
    hour = int(hour)
    minute = int(minute)
    second = int(second) if second else 0
    period = period.upper()
    if period == "PM" and hour != 12:
        hour += 12
    if period == "AM" and hour == 12:
        hour = 0
    return f"{hour:02d}:{minute:02d}:{second:02d}"
