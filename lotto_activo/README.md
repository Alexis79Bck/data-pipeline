# Lotto Activo Scraper

Scraper robusto para extraer datos históricos de la lotería Lotto Activo con procesamiento automático y almacenamiento estructurado.

## 🎯 Características

- **Scraping robusto**: Extracción de datos con manejo de errores y retry logic
- **Procesamiento inteligente**: Validación y normalización automática de datos
- **Almacenamiento dual**: Datos en `outputs/` y `data/` para diferentes usos
- **Manejo de errores**: Recuperación automática de fallos temporales
- **Logging detallado**: Monitoreo completo de operaciones
- **Validación de datos**: Control de calidad y consistencia

## 📁 Estructura de Directorios

```
lotto-activo/
├── scraper.py          # Implementación principal del scraper
├── requirements.txt    # Dependencias del proyecto
├── README.md          # Documentación
├── __init__.py        # Módulo Python
└── data/              # Datos procesados para API REST
    └── lotto_activo_YYYYMMDD_HHMMSS.json
```

## 🚀 Instalación

### 1. Instalar dependencias

```bash
pip install -r requirements.txt
```

### 2. Configurar entorno

```bash
# Crear directorios necesarios
mkdir -p outputs/lotto-activo
mkdir -p data/lotto-activo
mkdir -p logs
```

## 💻 Uso

### Uso básico

```python
from lotto_activo.scraper import LottoActivoScraper

# Crear instancia del scraper
scraper = LottoActivoScraper()

# Extraer datos para un rango de fechas
results = scraper.run("2025-01-01", "2025-01-07")

# Obtener datos más recientes (últimos 7 días)
recent_data = scraper.get_latest_data(days=7)
```

### Configuración avanzada

```python
# Configuración personalizada
scraper = LottoActivoScraper(
    name="lotto-activo-custom",
    url="https://custom-url.com/lotto/{start}/{end}/",
    max_retries=5,
    retry_delay=3.0,
    timeout=60,
    max_data_size_mb=100.0
)
```

### Uso con manejo de errores

```python
from lotto_activo.scraper import LottoActivoScraper
from common.base_scraper import ScraperError

try:
    scraper = LottoActivoScraper()
    results = scraper.run("2025-01-01", "2025-01-07")
    print(f"✅ Extraídos {len(results)} registros")
except ScraperError as e:
    print(f"❌ Error: {e}")
finally:
    scraper.close()
```

## 📊 Formato de Datos

### Datos de entrada (raw)
```json
{
    "fecha": "2025-01-15",
    "numero": "05",
    "animal": "LEON",
    "hora": "14:30:00",
    "fecha_raw": "15 de enero de 2025",
    "numero_raw": "5",
    "animal_raw": "León",
    "hora_raw": "2:30 PM",
    "fila": 1
}
```

### Datos procesados (output)
```json
{
    "fecha": "2025-01-15",
    "numero": "05",
    "animal": "LEON",
    "numero_map": "05",
    "fuente": "lotto-activo",
    "scraper": "lotto-activo",
    "procesado_en": "2025-01-15T14:30:00.123456",
    "fila": 1,
    "hora": "14:30:00",
    "validado": true
}
```

## 🔧 Configuración

### Variables de entorno

```bash
# Configuración opcional
export LOTTO_ACTIVO_URL="https://custom-url.com/lotto/{start}/{end}/"
export LOTTO_ACTIVO_MAX_RETRIES="5"
export LOTTO_ACTIVO_TIMEOUT="60"
export LOTTO_ACTIVO_MAX_DATA_SIZE_MB="100"
```

### Parámetros del scraper

| Parámetro | Tipo | Default | Descripción |
|-----------|------|---------|-------------|
| `name` | str | "lotto-activo" | Nombre del scraper |
| `url` | str | URL por defecto | URL base con placeholders |
| `max_retries` | int | 3 | Número máximo de reintentos |
| `retry_delay` | float | 2.0 | Delay entre reintentos (segundos) |
| `timeout` | int | 30 | Timeout para requests (segundos) |
| `max_data_size_mb` | float | 50.0 | Tamaño máximo de datos (MB) |

## 📈 Monitoreo y Logging

### Niveles de log

- **INFO**: Operaciones normales
- **WARNING**: Problemas menores recuperables
- **ERROR**: Errores que requieren atención
- **DEBUG**: Información detallada para debugging

### Métricas disponibles

```python
# Obtener métricas del último scraping
metrics = scraper._calculate_metrics(output_file)
print(f"Duración: {metrics['duration_seconds']}s")
print(f"Registros: {metrics['total_records']}")
print(f"Éxito: {metrics['success_rate']:.2%}")
```

### Estado del scraper

```python
# Verificar estado actual
status = scraper.get_status()
print(f"Estado: {status}")
```

## 🧪 Testing

### Ejecutar tests

```bash
# Tests unitarios
pytest test/test_lotto_activo_scraper.py -v

# Tests con cobertura
pytest test/test_lotto_activo_scraper.py --cov=lotto_activo --cov-report=html

# Tests de integración
pytest test/test_integration.py -v
```

### Tests disponibles

- **Test de scraping**: Verificación de extracción de datos
- **Test de procesamiento**: Validación de transformación de datos
- **Test de guardado**: Verificación de persistencia
- **Test de errores**: Manejo de casos de error
- **Test de validación**: Control de calidad de datos

## 🔍 Troubleshooting

### Problemas comunes

#### 1. Error de conexión
```
ScrapingError: Error de red al consultar URL
```
**Solución**: Verificar conectividad y URL, ajustar timeout

#### 2. Datos no encontrados
```
WARNING: No se encontraron datos en el rango especificado
```
**Solución**: Verificar rango de fechas y selectores HTML

#### 3. Error de validación
```
ValidationError: Rango de fechas inválido
```
**Solución**: Usar formato de fecha YYYY-MM-DD

### Debugging

```python
import logging

# Habilitar logging detallado
logging.basicConfig(level=logging.DEBUG)

# Usar scraper con debug
scraper = LottoActivoScraper()
scraper.logger.setLevel(logging.DEBUG)
```

## 📚 API Reference

### LottoActivoScraper

#### Métodos principales

- `run(start_date, end_date)`: Ejecuta scraping completo
- `get_latest_data(days=7)`: Obtiene datos recientes
- `close()`: Cierra sesión de requests

#### Métodos internos

- `scrape_data(start_date, end_date)`: Extrae datos crudos
- `process_data(raw_data)`: Procesa y valida datos
- `save_data(processed_data)`: Guarda datos procesados

### Excepciones

- `ScrapingError`: Error durante extracción
- `ProcessingError`: Error durante procesamiento
- `SavingError`: Error durante guardado
- `ValidationError`: Error de validación de datos

## 🤝 Contribución

### Estructura del código

1. **Scraping**: Extracción de datos de la web
2. **Procesamiento**: Validación y normalización
3. **Guardado**: Persistencia en archivos
4. **Manejo de errores**: Recuperación y logging

### Guías de desarrollo

1. Seguir PEP 8 para estilo de código
2. Agregar tests para nuevas funcionalidades
3. Documentar cambios en README
4. Usar type hints en funciones públicas

## 📄 Licencia

Este proyecto es parte del sistema de data pipeline para loterías y está sujeto a las políticas internas de la organización.

## 🔗 Enlaces relacionados

- [BaseScraper Documentation](../common/README.md)
- [Utils Documentation](../common/utils.py)
- [Config Documentation](../common/config.py)
- [Test Suite](../test/README.md)
