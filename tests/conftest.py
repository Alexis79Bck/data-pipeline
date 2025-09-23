# data-pipeline/test/conftest.py

import pytest
import sys
from pathlib import Path
import common.config as config

@pytest.fixture(autouse=True)
def patch_config_dirs(tmp_path, monkeypatch):
    """
    Fixture global que reemplaza las rutas de LOGS_DIR, OUTPUTS_DIR y DATA_DIR
    para que todas las pruebas usen carpetas temporales.
    Esto evita que los tests escriban en los directorios reales del proyecto.
    """
    
    # Crea una ruta temporal para cada directorio
    sys.path.append(str(Path(__file__).resolve().parent.parent))
    temp_logs = tmp_path / "logs"
    temp_outputs = tmp_path / "outputs"
    temp_data = tmp_path / "data"

    # Crear carpetas temporales
    temp_logs.mkdir(parents=True, exist_ok=True)
    temp_outputs.mkdir(parents=True, exist_ok=True)
    temp_data.mkdir(parents=True, exist_ok=True)

    # Reemplazar rutas en la configuración
    monkeypatch.setattr(config, "LOGS_DIR", temp_logs)
    monkeypatch.setattr(config, "OUTPUTS_DIR", temp_outputs)
    monkeypatch.setattr(config, "DATA_DIR", temp_data)

    yield  # 🔹 pytest liberará las rutas al terminar cada test
