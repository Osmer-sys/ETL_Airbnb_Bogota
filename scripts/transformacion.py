"""
================================================================
 Taller Evaluativo 2 - Proceso ETL con datasets de Airbnb
 Archivo  : transformacion.py
 Clase    : Transformacion
 Autor    : Pipeline ETL Airbnb Bogotá
 Fecha    : 2026-10-03
================================================================
Descripción:
    Módulo de Transformación (T) del pipeline ETL.
    Implementa la clase Transformacion que toma los DataFrames
    extraídos de MongoDB (listings, reviews, calendar) y aplica
    limpieza, normalización, conversión de tipos, derivación de
    variables, categorización y resolución de nulos/duplicados.

Entregables:
    - Archivo `scripts/transformacion.py` con la clase `Transformacion`.
    - Generación de DataFrames transformados e integración de logs.
================================================================
"""

import os
import sys
import logging
import json
import ast
import re
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Tuple, Any

import pandas as pd
import numpy as np
from dotenv import load_dotenv

# ── Rutas base ────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LOGS_DIR = BASE_DIR / "logs"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
LOGS_DIR.mkdir(exist_ok=True)
PROCESSED_DIR.mkdir(exist_ok=True)


# ── Logger del módulo ─────────────────────────────────────────
def _crear_logger(nombre: str, log_file: Path) -> logging.Logger:
    """Crea y retorna un logger configurado para archivo y consola."""
    logger = logging.getLogger(nombre)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    try:
        import io as _io
        _stream = open(sys.stdout.fileno(), mode="w", encoding="utf-8", buffering=1, errors="replace")
    except (AttributeError, _io.UnsupportedOperation):
        _stream = sys.stdout

    ch = logging.StreamHandler(_stream)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ═══════════════════════════════════════════════════════════════
class Transformacion:
    """
    Clase responsable de la capa de Transformación (T) del pipeline ETL.

    Responsabilidades:
    ------------------
    1. Limpieza de nulos y verificación de duplicados.
    2. Normalización de precios: remover '$' y ',', convertir a float (COP).
    3. Conversión de fechas al formato estándar YYYY-MM-DD.
    4. Derivación de variables temporales (año, mes, día, trimestre, día de la semana).
    5. Categorización de precios por rangos (Económico, Medio, Alto, Lujo).
    6. Expansión/tratamiento de campos anidados (ej. amenities -> n_amenities).
    7. Generación de DataFrames limpios y guardado opcional en data/processed/.
    8. Registro detallado de logs pre/post transformación.
    """

    def __init__(self, log_filename: Optional[str] = None):
        """Inicializa la clase Transformacion y su logger."""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fname = log_filename or f"transformacion_{timestamp}.log"
        self.log_file = LOGS_DIR / fname
        self.log = _crear_logger(f"Transformacion_{timestamp}", self.log_file)

        self.log.info("=" * 60)
        self.log.info(" CLASE TRANSFORMACION - Inicializada")
        self.log.info(f" Archivo de log: {self.log_file.name}")
        self.log.info("=" * 60)

    # ──────────────────────────────────────────────────────────
    # Helper: Normalización de Precio
    # ──────────────────────────────────────────────────────────
    @staticmethod
    def limpiar_precio(val: Any) -> Optional[float]:
        """
        Elimina caracteres no numéricos ($, comas) de un string de precio
        y lo convierte a float.
        """
        if pd.isna(val) or val is None:
            return None
        val_str = str(val).strip()
        val_clean = re.sub(r"[^\d.]", "", val_str.replace(",", ""))
        try:
            return float(val_clean) if val_clean else None
        except ValueError:
            return None

    # ──────────────────────────────────────────────────────────
    # Helper: Categorización de Precios
    # ──────────────────────────────────────────────────────────
    @staticmethod
    def categorizar_precio(precio: Optional[float], q25: float = 60000, q50: float = 120000, q75: float = 250000) -> str:
        """
        Categoriza el precio de un alojamiento en COP:
        - Económico : < q25
        - Medio     : q25 <= precio < q50
        - Alto      : q50 <= precio < q75
        - Lujo      : >= q75
        - Desconocido: None
        """
        if pd.isna(precio) or precio is None or precio <= 0:
            return "No especificado"
        if precio < q25:
            return "Económico"
        elif precio < q50:
            return "Medio"
        elif precio < q75:
            return "Alto"
        else:
            return "Lujo"

    # ──────────────────────────────────────────────────────────
    # Helper: Conteo de Amenities
    # ──────────────────────────────────────────────────────────
    @staticmethod
    def contar_amenities(val: Any) -> int:
        """Parsea la lista de amenities (string JSON) y retorna la cantidad de elementos."""
        if pd.isna(val) or not val:
            return 0
        val_str = str(val).strip()
        try:
            parsed = json.loads(val_str)
            if isinstance(parsed, list):
                return len(parsed)
        except Exception:
            pass
        try:
            parsed = ast.literal_eval(val_str)
            if isinstance(parsed, (list, set, tuple)):
                return len(parsed)
        except Exception:
            pass
        return 0

    # ──────────────────────────────────────────────────────────
    # 1. Transformación de LISTINGS
    # ──────────────────────────────────────────────────────────
    def transformar_listings(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica todas las transformaciones a la colección 'listings':
        - Deduplicación por 'id'
        - Eliminación de columnas con >95% nulos (neighbourhood_group_cleansed, license)
        - Normalización del campo 'price' a float64
        - Categorización de precio en rangos
        - Conversión de fechas a YYYY-MM-DD y derivación de variables de fecha
        - Expansión de 'amenities' a 'n_amenities'
        - Mapeo de booleanos ('t'/'f' -> True/False)
        - Imputación/tratamiento razonable de nulos numéricos (bedrooms, beds, bathrooms)
        """
        self.log.info("\n" + "=" * 50)
        self.log.info(" TRANSFORMANDO: LISTINGS")
        self.log.info("=" * 50)

        filas_inicio, cols_inicio = df_raw.shape
        self.log.info(f"Registros iniciales: {filas_inicio:,} | Columnas: {cols_inicio}")

        df = df_raw.copy()

        # 1.1 Deduplicación
        duplicados = df.duplicated(subset=["id"]).sum()
        if duplicados > 0:
            df = df.drop_duplicates(subset=["id"], keep="first")
            self.log.info(f"[listings] Registros duplicados eliminados: {duplicados:,}")
        else:
            self.log.info("[listings] 0 duplicados encontrados por 'id'.")

        # 1.2 Eliminar columnas con >95% de nulos
        cols_eliminar = [c for c in ["neighbourhood_group_cleansed", "license", "calendar_updated"] if c in df.columns]
        if cols_eliminar:
            df.drop(columns=cols_eliminar, inplace=True, errors="ignore")
            self.log.info(f"[listings] Columnas eliminadas por alto % de nulos: {cols_eliminar}")

        # 1.3 Normalización de precio
        self.log.info("[listings] Normalizando campo 'price'...")
        df["price_clean"] = df["price"].apply(self.limpiar_precio)
        nulos_precio = df["price_clean"].isnull().sum()
        self.log.info(f"[listings] Precio limpio - Mediana: ${df['price_clean'].median():,.2f} COP | Nulos: {nulos_precio}")

        # 1.4 Categorización de precio por cuartiles
        q25 = df["price_clean"].quantile(0.25) or 60000
        q50 = df["price_clean"].quantile(0.50) or 120000
        q75 = df["price_clean"].quantile(0.75) or 250000

        self.log.info(f"[listings] Cuartiles de precio para categorización: Q25=${q25:,.0f}, Q50=${q50:,.0f}, Q75=${q75:,.0f}")
        df["price_category"] = df["price_clean"].apply(lambda p: self.categorizar_precio(p, q25, q50, q75))
        self.log.info(f"[listings] Distribución categorización precio:\n{df['price_category'].value_counts().to_string()}")

        # 1.5 Tratamiento de amenities (campo anidado)
        self.log.info("[listings] Desanidando y contando amenities...")
        df["n_amenities"] = df["amenities"].apply(self.contar_amenities)
        self.log.info(f"[listings] Promedio de amenidades por listing: {df['n_amenities'].mean():.1f}")

        # 1.6 Conversión de fechas y derivación de variables
        fechas_cols = ["host_since", "first_review", "last_review", "calendar_last_scraped", "last_scraped"]
        for col_f in fechas_cols:
            if col_f in df.columns:
                df[col_f] = pd.to_datetime(df[col_f], errors="coerce")

        # Derivar año, mes, trimestre de host_since
        if "host_since" in df.columns:
            df["host_since_year"] = df["host_since"].dt.year
            df["host_since_month"] = df["host_since"].dt.month
            df["host_since_quarter"] = df["host_since"].dt.quarter
            # Convertir objeto Timestamp a string formato YYYY-MM-DD
            df["host_since"] = df["host_since"].dt.strftime("%Y-%m-%d")

        if "first_review" in df.columns:
            df["first_review"] = df["first_review"].dt.strftime("%Y-%m-%d")

        if "last_review" in df.columns:
            df["last_review_year"] = df["last_review"].dt.year
            df["last_review"] = df["last_review"].dt.strftime("%Y-%m-%d")

        self.log.info("[listings] Fechas estandarizadas a YYYY-MM-DD y variables temporales derivadas.")

        # 1.7 Conversión de booleanos ('t'/'f' -> True/False)
        bool_cols = ["host_is_superhost", "host_has_profile_pic", "host_identity_verified", "has_availability", "instant_bookable"]
        for bcol in bool_cols:
            if bcol in df.columns:
                df[bcol] = df[bcol].map({"t": True, "f": False, True: True, False: False}).astype("boolean")

        # 1.8 Conversión de columnas numéricas clave e imputación básica
        num_cols_map = {
            "accommodates": 1,
            "bedrooms": 1,
            "beds": 1,
            "bathrooms": 1,
            "minimum_nights": 1,
            "maximum_nights": 365,
            "availability_365": 0,
            "number_of_reviews": 0,
            "review_scores_rating": None,
        }
        for ncol, default_val in num_cols_map.items():
            if ncol in df.columns:
                df[ncol] = pd.to_numeric(df[ncol], errors="coerce")
                if default_val is not None:
                    df[ncol] = df[ncol].fillna(default_val)

        filas_fin, cols_fin = df.shape
        self.log.info(f"[listings] Transformación completada: {filas_fin:,} filas × {cols_fin} columnas.")
        return df

    # ──────────────────────────────────────────────────────────
    # 2. Transformación de REVIEWS
    # ──────────────────────────────────────────────────────────
    def transformar_reviews(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones a la colección 'reviews':
        - Deduplicación por 'id'
        - Conversión de 'date' a YYYY-MM-DD
        - Derivación de variables temporales: año, mes, día, trimestre, día de semana
        - Limpieza de nulos en comentarios
        """
        self.log.info("\n" + "=" * 50)
        self.log.info(" TRANSFORMANDO: REVIEWS")
        self.log.info("=" * 50)

        filas_inicio, cols_inicio = df_raw.shape
        self.log.info(f"Registros iniciales: {filas_inicio:,} | Columnas: {cols_inicio}")

        df = df_raw.copy()

        # 2.1 Deduplicación
        duplicados = df.duplicated(subset=["id"]).sum() if "id" in df.columns else 0
        if duplicados > 0:
            df = df.drop_duplicates(subset=["id"], keep="first")
            self.log.info(f"[reviews] Registros duplicados eliminados: {duplicados:,}")
        else:
            self.log.info("[reviews] 0 duplicados encontrados.")

        # 2.2 Conversión de fechas
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")

        # Derivación de variables temporales
        df["year"] = df["date_dt"].dt.year
        df["month"] = df["date_dt"].dt.month
        df["day"] = df["date_dt"].dt.day
        df["quarter"] = df["date_dt"].dt.quarter
        df["day_of_week"] = df["date_dt"].dt.dayofweek  # 0=Lunes, 6=Domingo
        df["is_weekend"] = df["day_of_week"].isin([5, 6]).astype(int)

        # Formatear 'date' a YYYY-MM-DD string
        df["date"] = df["date_dt"].dt.strftime("%Y-%m-%d")
        df.drop(columns=["date_dt"], inplace=True)

        # 2.3 Limpieza de comentarios
        df["comments"] = df["comments"].fillna("Sin comentario").astype(str)

        # 2.4 Asegurar tipos de ID
        if "listing_id" in df.columns:
            df["listing_id"] = pd.to_numeric(df["listing_id"], errors="coerce")
        if "reviewer_id" in df.columns:
            df["reviewer_id"] = pd.to_numeric(df["reviewer_id"], errors="coerce")

        filas_fin, cols_fin = df.shape
        self.log.info(f"[reviews] Transformación completada: {filas_fin:,} filas × {cols_fin} columnas.")
        return df

    # ──────────────────────────────────────────────────────────
    # 3. Transformación de CALENDAR
    # ──────────────────────────────────────────────────────────
    def transformar_calendar(self, df_raw: pd.DataFrame) -> pd.DataFrame:
        """
        Aplica transformaciones a la colección 'calendar':
        - Deduplicación por (listing_id, date)
        - Mapeo de 'available': 't' -> True (1), 'f' -> False (0)
        - Conversión de 'date' a YYYY-MM-DD
        - Derivación de variables temporales: año, mes, semana, trimestre
        - Normalización numérica de minimum_nights y maximum_nights
        """
        self.log.info("\n" + "=" * 50)
        self.log.info(" TRANSFORMANDO: CALENDAR")
        self.log.info("=" * 50)

        filas_inicio, cols_inicio = df_raw.shape
        self.log.info(f"Registros iniciales: {filas_inicio:,} | Columnas: {cols_inicio}")

        df = df_raw.copy()

        # 3.1 Deduplicación
        duplicados = df.duplicated(subset=["listing_id", "date"]).sum()
        if duplicados > 0:
            df = df.drop_duplicates(subset=["listing_id", "date"], keep="first")
            self.log.info(f"[calendar] Registros duplicados eliminados: {duplicados:,}")
        else:
            self.log.info("[calendar] 0 duplicados encontrados por (listing_id, date).")

        # 3.2 Conversión de disponible (boolean)
        df["is_available"] = df["available"].map({"t": True, "f": False, True: True, False: False}).astype("boolean")

        # 3.3 Conversión de fechas y derivación de variables
        df["date_dt"] = pd.to_datetime(df["date"], errors="coerce")
        df["year"] = df["date_dt"].dt.year
        df["month"] = df["date_dt"].dt.month
        df["week"] = df["date_dt"].dt.isocalendar().week.astype(int)
        df["quarter"] = df["date_dt"].dt.quarter
        df["day_of_week"] = df["date_dt"].dt.dayofweek

        # Formato YYYY-MM-DD
        df["date"] = df["date_dt"].dt.strftime("%Y-%m-%d")
        df.drop(columns=["date_dt"], inplace=True)

        # 3.4 Conversión numérica de campos de noches
        for col_n in ["minimum_nights", "maximum_nights", "listing_id"]:
            if col_n in df.columns:
                df[col_n] = pd.to_numeric(df[col_n], errors="coerce")

        filas_fin, cols_fin = df.shape
        self.log.info(f"[calendar] Transformación completada: {filas_fin:,} filas × {cols_fin} columnas.")
        return df

    # ──────────────────────────────────────────────────────────
    # 3.1 Resumen y agregación de CALENDAR (por mes y semana)
    # ──────────────────────────────────────────────────────────
    def resumir_calendario_mensual(self, df_calendar_clean: pd.DataFrame) -> pd.DataFrame:
        """
        Agrupa y resume la disponibilidad del calendario por mes:
        - Total de días registrados
        - Total de días disponibles
        - Tasa de disponibilidad (%)
        - Promedio de noches mínimas
        """
        self.log.info("[calendar] Agrupando y resumiendo calendario por año y mes...")
        df_disp = df_calendar_clean.copy()
        if "is_available" in df_disp.columns:
            df_disp["disp_num"] = df_disp["is_available"].astype(int)
        else:
            df_disp["disp_num"] = df_disp["available"].map({"t": 1, "f": 0}).fillna(0)

        resumen = (
            df_disp.groupby(["year", "month"])
            .agg(
                total_dias=("date", "count"),
                dias_disponibles=("disp_num", "sum"),
                disponibilidad_pct=("disp_num", "mean"),
                noches_minimas_prom=("minimum_nights", "mean"),
            )
            .reset_index()
        )
        resumen["disponibilidad_pct"] = (resumen["disponibilidad_pct"] * 100).round(2)
        resumen["noches_minimas_prom"] = resumen["noches_minimas_prom"].round(1)
        self.log.info(f"[calendar] Resumen mensual generado: {len(resumen)} meses analizados.")
        return resumen

    # ──────────────────────────────────────────────────────────
    # 4. Método principal: transformar_todo()
    # ──────────────────────────────────────────────────────────
    def transformar_todo(self, dataframes_raw: Dict[str, pd.DataFrame]) -> Dict[str, pd.DataFrame]:
        """
        Ejecuta el pipeline completo de transformación para las 3 colecciones:
        listings, reviews y calendar.

        Parametros:
        -----------
        dataframes_raw : dict
            Diccionario con las claves 'listings', 'reviews', 'calendar'
            conteniendo los DataFrames extraídos de MongoDB.

        Retorna:
        --------
        Dict[str, pd.DataFrame] con los DataFrames limpios y transformados.
        """
        self.log.info("=" * 60)
        self.log.info(" INICIANDO PROCESO GLOBAL DE TRANSFORMACION")
        self.log.info("=" * 60)

        t_inicio = datetime.now()
        dataframes_clean = {}

        if "listings" in dataframes_raw:
            dataframes_clean["listings"] = self.transformar_listings(dataframes_raw["listings"])

        if "reviews" in dataframes_raw:
            dataframes_clean["reviews"] = self.transformar_reviews(dataframes_raw["reviews"])

        if "calendar" in dataframes_raw:
            cal_clean = self.transformar_calendar(dataframes_raw["calendar"])
            dataframes_clean["calendar"] = cal_clean
            dataframes_clean["calendar_summary"] = self.resumir_calendario_mensual(cal_clean)

        duracion = (datetime.now() - t_inicio).total_seconds()

        self.log.info("\n" + "=" * 60)
        self.log.info(" RESUMEN FINAL DE TRANSFORMACION")
        self.log.info("=" * 60)
        for nombre, df in dataframes_clean.items():
            self.log.info(f"  ✓ {nombre:<12}: {len(df):>10,} registros | {len(df.columns):>3} columnas")
        self.log.info(f"  Tiempo total: {duracion:.2f}s")
        self.log.info("=" * 60)

        return dataframes_clean

    # ──────────────────────────────────────────────────────────
    # Guardado de resultados procesados
    # ──────────────────────────────────────────────────────────
    def guardar_procesados(self, dfs: Dict[str, pd.DataFrame]) -> None:
        """Guarda los DataFrames transformados en data/processed/ en formato CSV/Parquet."""
        self.log.info("Guardando DataFrames transformados en data/processed/...")
        for nombre, df in dfs.items():
            out_csv = PROCESSED_DIR / f"{nombre}_clean.csv"
            df.to_csv(out_csv, index=False, encoding="utf-8")
            self.log.info(f"  Saved: {out_csv.name} ({out_csv.stat().st_size / (1024**2):.1f} MB)")


# ══════════════════════════════════════════════════════════════
# Ejecución directa para pruebas del script
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    from extraccion import Extraccion

    print("\n" + "=" * 60)
    print(" PRUEBA DE LA CLASE Transformacion")
    print("=" * 60)

    # 1. Extraer datos
    ext = Extraccion()
    ext.conectar()

    print("\nExtrayendo muestras...")
    df_listings_raw = ext.extraer_listings(limite=500)
    df_reviews_raw = ext.extraer_reviews(limite=1000)
    df_calendar_raw = ext.extraer_calendar(limite=5000)
    ext.desconectar()

    # 2. Transformar
    t = Transformacion()
    dfs_limpios = t.transformar_todo({
        "listings": df_listings_raw,
        "reviews": df_reviews_raw,
        "calendar": df_calendar_raw,
    })

    print("\n--- Verificación Muestra Listings ---")
    df_l = dfs_limpios["listings"]
    print(df_l[["id", "name", "price_clean", "price_category", "n_amenities", "host_since_year"]].head(3))

    print("\n--- Verificación Muestra Reviews ---")
    df_r = dfs_limpios["reviews"]
    print(df_r[["listing_id", "date", "year", "month", "quarter", "is_weekend"]].head(3))

    print("\n--- Verificación Muestra Calendar ---")
    df_c = dfs_limpios["calendar"]
    print(df_c[["listing_id", "date", "is_available", "year", "month", "week"]].head(3))

    print("\n" + "=" * 60)
    print(" Clase Transformacion - FUNCIONAL")
    print("=" * 60)
