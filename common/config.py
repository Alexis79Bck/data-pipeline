# data-pipeline/common/config.py
"""Configuration settings and constants for the data pipeline."""

import os
from pathlib import Path

# Ruta base del proyecto
BASE_DIR = Path(__file__).parent.parent

# Rutas de salida
OUTPUTS_DIR = BASE_DIR / "outputs"
LOGS_DIR = BASE_DIR / "logs"
DATA_DIR = BASE_DIR / "data"

# Asegurar que las carpetas existan
OUTPUTS_DIR.mkdir(exist_ok=True)
LOGS_DIR.mkdir(exist_ok=True)
DATA_DIR.mkdir(exist_ok=True)

# Formatos
DATE_FORMAT = "%Y-%m-%d"
TIME_FORMAT = "%H:%M:%S"
DATETIME_FORMAT = "%Y-%m-%d %H:%M:%S"

# Mapeo número → animal (completo para Lotto Activo)
ANIMALS_MAP = {
    "0": "DELFIN",
    "00": "BALLENA",
    "01": "CARNERO",
    "02": "TORO",
    "03": "CIEMPIES",
    "04": "ALACRAN",
    "05": "LEON",
    "06": "RANA",
    "07": "PERICO",
    "08": "RATON",
    "09": "AGUILA",
    "10": "TIGRE",
    "11": "GATO",
    "12": "CABALLO",
    "13": "MONO",
    "14": "PALOMA",
    "15": "ZORRO",
    "16": "OSO",
    "17": "PAVO",
    "18": "BURRO",
    "19": "CHIVO",
    "20": "COCHINO",
    "21": "GALLO",
    "22": "CAMELLO",
    "23": "CEBRA",
    "24": "IGUANA",
    "25": "GALLINA",
    "26": "VACA",
    "27": "PERRO",
    "28": "ZAMURO",
    "29": "ELEFANTE",
    "30": "CAIMAN",
    "31": "LAPA",
    "32": "ARDILLA",
    "33": "PESCADO",
    "34": "VENADO",
    "35": "JIRAFA",
    "36": "CULEBRA"
}

# Inverso: animal → número
ANIMAL_TO_NUMBER = {
    animal: number for number, animal in ANIMALS_MAP.items()
}
