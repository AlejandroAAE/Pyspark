# Mini PySpark Data Pipeline — Taxi Profiler

Pequeño proyecto demostrativo de procesamiento de datos con PySpark, que implementa un flujo ETL básico: lectura, limpieza, enriquecimiento, validación, exportación y visualización.

Este proyecto forma parte de mi portfolio como desarrollador Python, demostrando conocimientos de ingeniería de datos.

---

## Objetivo del proyecto

1. Cargar datos crudos desde CSV  
2. Limpiarlos y convertir tipos  
3. Crear columnas derivadas útiles  
4. Calcular métricas de calidad  
5. Guardar datos transformados  
6. Realizar análisis visual exploratorio (EDA)

---

## Tecnologías usadas

- Python
- PySpark
- Pandas
- Matplotlib
- Click (CLI)

---

## Estructura del proyecto

Pyspark/
├─ pipeline/
│ ├─ cli.py ← CLI del pipeline
│ ├─ transform.py ← Limpieza y enriquecimiento
│ └─ quality.py ← Métricas de calidad
│
├─ data/
│ └─ raw/
│ └─ taxi_sample.csv
│
├─ processed/
│ └─ data/
│ └─ cleaned.csv
│
├─ visualize.py ← Script de visualización
├─ requirements.txt
└─ README.md

## Ejecución del pipeline

Ejecutar el siguiente comando:


python -m pipeline.cli run --input data/raw/taxi_sample.csv --outdir processed

Salida esperada:


=== DATA QUALITY ===
{ nulls, duplicates, summary ... }
OK ✓ CSV escrito en: processed/data/cleaned.csv

