# Módulo Data Pipeline para el Sistema de Predicción de Animalitos

Este módulo es una parte fundamental del Sistema de Predicción de Animalitos. Está desarrollado en Python y actúa como un **Data Pipeline** encargado de realizar web scraping para la obtención de resultados históricos desde una URL predeterminada. Además, tiene la responsabilidad de estructurar los datos recolectados y responder a las solicitudes realizadas por la API Backend desarrollada en Laravel.

## Funcionalidades principales

- **Web Scraper**: Obtiene resultados históricos de sorteos desde una fuente externa.
- **Estructuración de datos**: Procesa y organiza los datos recolectados en un formato adecuado para su consumo por la API Backend.
- **Interoperabilidad**: Responde a las solicitudes de la API Backend, proporcionando los datos históricos necesarios para los cálculos estadísticos y probabilísticos.

## Requisitos previos

Asegúrate de tener instalados los siguientes componentes antes de comenzar:

- Python >= 3.8
- pip (gestor de paquetes de Python)

## Instalación

1. Clona este repositorio:

   ```bash
   git clone https://github.com/Alexis79Bck/animalitos-predictions-system.git
   cd animalitos-predictions-system/data-pipeline
   ```

2. Instala las dependencias necesarias:

   ```bash
   pip install -r requirements.txt
   ```

## Uso

### Ejecución del Web Scraper

Para ejecutar el web scraper y recolectar datos históricos, utiliza el siguiente comando:

```bash
python main.py
```

### Configuración

La configuración del módulo, como la URL predeterminada para el scraping y otros parámetros, se encuentra en el archivo `common/config.py`. Asegúrate de ajustar estos valores según tus necesidades.

### Logs

Los logs generados durante la ejecución del módulo se almacenan en la carpeta `logs/`. Estos incluyen información sobre errores, resultados procesados y otros eventos importantes.

## Estructura del Proyecto

- `main.py`: Punto de entrada principal para ejecutar el Data Pipeline.
- `common/`: Contiene utilidades compartidas, configuraciones y clases base.
- `lotto_activo/`: Implementaciones específicas del scraper y procesamiento de datos.
- `data/`: Carpeta para almacenar temporalmente los datos recolectados.
- `logs/`: Carpeta para almacenar los archivos de log generados durante la ejecución.

## Contribuciones

Si deseas contribuir a este módulo, por favor sigue estos pasos:

1. Haz un fork del repositorio.
2. Crea una nueva rama para tu funcionalidad o corrección de errores:
   ```bash
   git checkout -b feature/nueva-funcionalidad
   ```
3. Realiza tus cambios y haz commit:
   ```bash
   git commit -m "Descripción de los cambios"
   ```
4. Envía tus cambios al repositorio remoto:
   ```bash
   git push origin feature/nueva-funcionalidad
   ```
5. Abre un Pull Request en GitHub.

## Licencia

Este módulo está licenciado bajo la Licencia Apache 2.0. Consulta el archivo `LICENSE` en la raíz del repositorio para más detalles.