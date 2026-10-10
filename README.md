# ETL Airbnb Bogotá

## Descripción del proyecto y su objetivo

Proyecto de Inteligencia de Negocios (ITM) que implementa un proceso ETL completo con Python y pandas sobre los datasets de Airbnb de Bogotá. El flujo extrae las colecciones Listings, Reviews y Calendar desde MongoDB local, evalúa su calidad con un análisis exploratorio (EDA), aplica limpieza y transformaciones (precios, fechas, categorías, campos anidados) y carga los resultados en SQLite (`data/airbnb_bogota.db`), además de exportar reportes a Excel (`data/exports/*.xlsx`).

**Objetivo:** automatizar la extracción, limpieza y carga de los datos de Airbnb Bogotá para dejarlos listos para el análisis (oferta, precios, disponibilidad y reseñas), con trazabilidad total mediante logs por ejecución.

## Instrucciones de instalación

### 1. Creación del entorno virtual

```bash
python -m venv .venv
# Windows PowerShell
.venv\Scripts\Activate.ps1
```

En Linux o macOS, active el entorno con `source .venv/bin/activate`.

### 2. Instalación de dependencias

```bash
pip install -r requirements.txt
copy .env.example .env
```

En Linux o macOS copie con `cp .env.example .env`. El `.env.example` ya viene con `MONGO_URI`, `MONGO_DB` y `SQLITE_PATH=data/airbnb_bogota.db`. Requiere MongoDB local en `mongodb://localhost:27017/`.

### 3. Ejecución del proyecto

Colocar `listings.csv.gz`, `calendar.csv.gz` y `reviews.csv.gz` en `data/raw/`, luego:

```bash
python scripts/01_ingest_to_mongodb.py   # CSV.GZ -> MongoDB
python scripts/03_transform_data.py      # MongoDB -> data/processed/*.csv
python scripts/04_load_to_sqlite.py      # CSV limpios -> SQLite + XLSX
```

El resultado queda en `data/airbnb_bogota.db` (4 tablas), en `data/processed/*_clean.csv` y en `data/exports/*.xlsx`.

## Reiniciar las fuentes

```bash
# Borra colecciones en MongoDB y vuelve a cargar desde cero
python scripts/01_ingest_to_mongodb.py
```

## Integrantes del grupo y responsabilidades

| Integrante | Responsabilidad |
|---|---|
| OSMER ANTONIO PADILLA OYOLA | Ingesta CSV.GZ a MongoDB y extracción (`01_ingest_to_mongodb.py`, `extraccion.py`, logs de ingesta/extracción) |
| JHON FERNANDO SANCHEZ ALVAREZ | Transformación (`transformacion.py`, `03_transform_data.py`: limpieza, precios, fechas, categorías) |
| JUAN ANDRES GUTIERREZ HINCAPIE | Carga a SQLite y exportación a Excel (`carga.py`, `04_load_to_sqlite.py`, validaciones) |
| KEVIN CARDENAS RIVILLAS | Análisis exploratorio (notebook `exploracion_airbnb.ipynb`), README e informe final |

## Ejemplo de ejecución del proceso ETL

```bash
python scripts/04_load_to_sqlite.py
```

Salida real (10-oct-2026, SQLite 3.35.5):

```text
[Paso 1/4] Leyendo conjuntos de datos procesados...
  • listings         : 19,187 filas
  • reviews          : 527,731 filas
  • calendar         : 500,000 filas
  • calendar_summary : 13 filas

[Paso 3/4] Cargando tablas en SQLite con relaciones y claves foráneas...
[listings] Carga completada: 19,187 registros insertados en 0.19s
[reviews] Carga completada: 527,731 registros en 26.77s
[calendar] Carga completada: 500,000 registros en 5.75s
[calendar_summary] Insertando 13 registros...

  ✓ listings          :     19,187 registros
  ✓ reviews           :    527,731 registros
  ✓ calendar          :    500,000 registros
  ✓ calendar_summary  :         13 registros

  ✓ Archivo generado: listings_procesados.xlsx (2.06 MB)
  ✓ Archivo generado: calendar_resumen_ejecutivo.xlsx
  ✓ Libro maestro multi-hoja generado: airbnb_reporte_ejecutivo.xlsx
```

Ejemplo de log de transformación (`logs/transformacion_20261010_072705.log`):

```text
2026-10-10 07:27:06 | INFO | Registros iniciales: 19,187 | Columnas: 90
2026-10-10 07:27:07 | INFO | [listings] Precio limpio - Mediana: $161,460.00 COP | Nulos: 195
2026-10-10 07:27:07 | INFO | [listings] Cuartiles de precio: Q25=$110,000, Q50=$161,460, Q75=$230,346
2026-10-10 07:27:09 | INFO | ✓ listings: 19,187 registros | 94 columnas
2026-10-10 07:27:09 | INFO | ✓ reviews: 527,731 registros | 12 columnas
2026-10-10 07:27:09 | INFO | ✓ calendar: 500,000 registros | 11 columnas
```

## Recorrido sugerido

1. Ejecutar el notebook `notebooks/exploracion_airbnb.ipynb`.
2. Comparar cada fase con los módulos de `scripts` (`extraccion.py`, `transformacion.py`, `carga.py`).
3. Ejecutar el pipeline completo (`01`, `03`, `04`).
4. Revisar los logs en `logs/` y los CSV en `data/processed/`.
5. Examinar las tablas en `data/airbnb_bogota.db` y los reportes en `data/exports/`.
