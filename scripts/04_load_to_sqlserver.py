"""
=============================================================
 Taller Evaluativo 2 — Fase 4: Ejecución de Carga Completa
 Script : 04_load_to_sqlserver.py
 Autor  : Pipeline ETL Airbnb Bogotá
 Fecha  : 2026-10-03
=============================================================
Descripción:
    Ejecuta el proceso completo de Carga (L):
    1. Lee los DataFrames procesados de `data/processed/`.
    2. Instancia la clase Carga y crea el esquema relacional en SQL Server.
    3. Carga la totalidad de los datos en SQL Server con Foreign Keys activas.
    4. Genera los archivos XLSX estructurados en `data/exports/`.
    5. Registra métricas y tiempos de ejecución en logs/.
=============================================================
"""

import sys
import time
from pathlib import Path
import pandas as pd

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "scripts"))

from carga import Carga

PROCESSED_DIR = BASE_DIR / "data" / "processed"

def main():
    print("=" * 65)
    print(" FASE 4 — PROCESAMIENTO ETL: CARGA SQL SERVER Y EXPORTACIÓN XLSX")
    print("=" * 65)

    # 1. Leer DataFrames limpios
    print("\n[Paso 1/4] Leyendo conjuntos de datos procesados...")
    t0 = time.time()
    df_listings = pd.read_csv(PROCESSED_DIR / "listings_clean.csv")
    df_reviews = pd.read_csv(PROCESSED_DIR / "reviews_clean.csv")
    df_calendar = pd.read_csv(PROCESSED_DIR / "calendar_clean.csv")
    df_summary = pd.read_csv(PROCESSED_DIR / "calendar_summary_clean.csv")

    print(f"  • listings         : {len(df_listings):,} filas")
    print(f"  • reviews          : {len(df_reviews):,} filas")
    print(f"  • calendar         : {len(df_calendar):,} filas")
    print(f"  • calendar_summary : {len(df_summary):,} filas")
    print(f"✓ Lectura completada en {time.time() - t0:.2f}s")

    # 2. Inicializar Carga y conectar a SQL Server
    print("\n[Paso 2/4] Conectando a SQL Server y creando esquema...")
    t1 = time.time()
    loader = Carga()
    if not loader.conectar():
        print("❌ Error de conexión a SQL Server. Abortando.")
        sys.exit(1)

    # 3. Cargar en SQL Server
    print("\n[Paso 3/4] Cargando tablas en SQL Server con relaciones y claves foráneas...")
    res = loader.cargar_todo({
        "listings": df_listings,
        "reviews": df_reviews,
        "calendar": df_calendar,
        "calendar_summary": df_summary,
    })
    print(f"✓ Carga en base de datos completada en {time.time() - t1:.2f}s")

    # 4. Exportar a XLSX
    print("\n[Paso 4/4] Exportando datos limpios a archivos Excel (.xlsx)...")
    t2 = time.time()
    loader.exportar_excel({
        "listings": df_listings,
        "reviews": df_reviews,
        "calendar_summary": df_summary,
    })
    print(f"✓ Exportación Excel completada en {time.time() - t2:.2f}s")

    loader.desconectar()

    print("\n" + "=" * 65)
    print(" RESUMEN FINAL FASE 4 — CARGA COMPLETA EXITOSA")
    print("=" * 65)
    for tbl, count in res.items():
        print(f"  ✓ dbo.{tbl:<18}: {count:>10,} registros")
    print(f"\nTiempo total de ejecución: {time.time() - t0:.2f}s")
    print("=" * 65)

if __name__ == "__main__":
    main()
