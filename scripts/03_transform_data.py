"""
=============================================================
 Taller Evaluativo 2 — Fase 3: Ejecución de Transformación
 Script : 03_transform_data.py
 Autor  : Pipeline ETL Airbnb Bogotá
 Fecha  : 2026-10-03
=============================================================
Descripción:
    Ejecuta el pipeline completo de Transformación (T):
    1. Extrae las colecciones desde MongoDB mediante la clase `Extraccion`.
    2. Aplica limpieza, normalización, derivaciones y categorizaciones
       mediante la clase `Transformacion`.
    3. Exporta los DataFrames limpios a `data/processed/` listos para la
       carga analítica en SQL Server y exportación a Excel.
=============================================================
"""

import sys
import time
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from extraccion import Extraccion
from transformacion import Transformacion

def main():
    print("=" * 65)
    print(" FASE 3 — PROCESAMIENTO ETL: TRANSFORMACIÓN DE DATOS")
    print("=" * 65)

    # 1. Extracción
    print("\n[Paso 1/3] Conectando a MongoDB y extrayendo colecciones...")
    t0 = time.time()
    ext = Extraccion()
    if not ext.conectar():
        print("❌ Error al conectar a MongoDB. Abortando.")
        sys.exit(1)

    print("  -> Extrayendo listings (completo)...")
    df_listings = ext.extraer_listings()

    print("  -> Extrayendo reviews (completo)...")
    df_reviews = ext.extraer_reviews()

    print("  -> Extrayendo calendar (500,000 registros para análisis temporal)...")
    df_calendar = ext.extraer_calendar(limite=500000)

    ext.desconectar()
    print(f"✓ Extracción completada en {time.time() - t0:.2f}s")

    # 2. Transformación
    print("\n[Paso 2/3] Aplicando transformaciones con la clase Transformacion...")
    t1 = time.time()
    trans = Transformacion()
    dfs_clean = trans.transformar_todo({
        "listings": df_listings,
        "reviews": df_reviews,
        "calendar": df_calendar,
    })
    print(f"✓ Transformación completada en {time.time() - t1:.2f}s")

    # 3. Guardado en data/processed/
    print("\n[Paso 3/3] Guardando DataFrames limpios en data/processed/...")
    t2 = time.time()
    trans.guardar_procesados(dfs_clean)
    print(f"✓ Guardado completado en {time.time() - t2:.2f}s")

    print("\n" + "=" * 65)
    print(" FASE 3 COMPLETADA EXITOSAMENTE")
    print("=" * 65)

if __name__ == "__main__":
    main()
