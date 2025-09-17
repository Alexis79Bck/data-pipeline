# lotto-activo/historical_loader.py
"""
Carga histórica de datos de Lotto Activo - Versión optimizada para data-pipeline
Extrae datos semanales y genera formato JSON normalizado para API Laravel BD.
"""

import json
import re
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional
import requests
from bs4 import BeautifulSoup

# Importaciones internas
from common.base_scraper import BaseScraper
from common.config import (
    ANIMALS_MAP,
    RESULTADOS_URLS,
    OUTPUTS_DIR,
    LOGS_DIR,
    DATA_DIR,
    DEFAULT_HEADERS
)

class HistoricalLoader:
    def __init__(self, source="LOTERIADEHOY"):
        self.base_url = RESULTADOS_URLS[source]
        
    def load_last_6_months(self):
        """Carga los últimos 6 meses de datos"""
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
        
        return self._load_data_for_range(start_date, end_date)
    
    def _load_data_for_range(self, start_date, end_date):
        """Carga datos para un rango de fechas específico"""
        url = self.base_url.format(start=start_date, end=end_date)
        
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.text, 'html.parser')
            table = soup.find('table', {'id': 'table'})
            
            if not table:
                return []
            
            # Extraer datos de la tabla (adaptar según estructura real)
            data = self._extract_table_data(table, start_date, end_date)
            
            # Guardar en JSON
            self._save_to_json(data, start_date, end_date)
            
            return data
            
        except Exception as e:
            print(f"Error loading data: {e}")
            return []
    
    def _extract_table_data(self, table, start_date, end_date):
        """Extrae datos de la tabla semanal"""
        data = []
        
        # Implementar lógica de extracción aquí
        # (Similar a la discutida anteriormente)
        
        return data
    
    def _save_to_json(self, data, start_date, end_date):
        """Guarda datos en formato JSON"""
        filename = f"historical_data_{start_date}_to_{end_date}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        
        print(f"Datos guardados en: {filename}")

# Ejecución única
if __name__ == "__main__":
    loader = HistoricalLoader()
    historical_data = loader.load_last_6_months()
    print(f"Carga completada. {len(historical_data)} registros obtenidos.")