"""
================================================================
 Taller Evaluativo 2 - Proceso ETL con datasets de Airbnb
 Archivo  : carga.py
 Clase    : Carga
 Autor    : Pipeline ETL Airbnb Bogotá
 Fecha    : 2026-10-03
================================================================
Descripción:
    Módulo de Carga (L) del pipeline ETL.
    Implementa la clase Carga que:
    1. Establece conexión a SQL Server (Database: AirbnbBogota).
    2. Crea las tablas relacionales con tipos de datos adecuados,
       claves primarias (PK), claves foráneas (FK) e índices.
    3. Carga los DataFrames limpios en las tablas de SQL Server
       utilizando inserción por lotes de alto rendimiento (fast_executemany).
    4. Exporta los conjuntos de datos limpios a archivos XLSX estructurados.
    5. Registra en un archivo de log cada paso del proceso, número
       de registros insertados por tabla, tiempos y posibles alertas.
================================================================
"""

import os
import sys
import logging
import urllib
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict, Any

import pandas as pd
import numpy as np
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# ── Rutas base ────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LOGS_DIR = BASE_DIR / "logs"
EXPORTS_DIR = BASE_DIR / "data" / "exports"
PROCESSED_DIR = BASE_DIR / "data" / "processed"

LOGS_DIR.mkdir(exist_ok=True)
EXPORTS_DIR.mkdir(exist_ok=True)


# ── Configuración de Logging ──────────────────────────────────
def _crear_logger(nombre: str, log_file: Path) -> logging.Logger:
    """Crea y retorna un logger configurado con manejadores de archivo y consola."""
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
class Carga:
    """
    Clase responsable de la capa de Carga (L) del pipeline ETL.

    Responsabilidades:
    ------------------
    - Gestionar la conexión a SQL Server mediante SQLAlchemy y pyodbc.
    - Definir y ejecutar el esquema DDL relacional (tablas, PK, FK, índices).
    - Cargar DataFrames limpios a SQL Server con soporte de fast_executemany.
    - Exportar datos analíticos a archivos Excel (.xlsx).
    - Registrar métricas de inserción en log por cada ejecución.
    """

    def __init__(
        self,
        server: Optional[str] = None,
        database: Optional[str] = None,
        driver: Optional[str] = None,
        trusted_connection: str = "yes",
    ):
        """Inicializa la clase Carga con las configuraciones de conexión."""
        self.server = server or os.getenv("SQL_SERVER", "localhost")
        self.database = database or os.getenv("SQL_DATABASE", "AirbnbBogota")
        self.driver = driver or os.getenv("SQL_DRIVER", "ODBC Driver 18 for SQL Server")
        self.trusted_connection = trusted_connection

        self.engine: Optional[Engine] = None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = LOGS_DIR / f"carga_sqlserver_{timestamp}.log"
        self.log = _crear_logger(f"Carga_{timestamp}", self.log_file)

        self.log.info("=" * 60)
        self.log.info(" CLASE CARGA - Inicializada")
        self.log.info(f" Servidor    : {self.server}")
        self.log.info(f" Base Datos  : {self.database}")
        self.log.info(f" Driver ODBC : {self.driver}")
        self.log.info(f" Archivo Log : {self.log_file.name}")
        self.log.info("=" * 60)

    # ──────────────────────────────────────────────────────────
    # Conexión a SQL Server
    # ──────────────────────────────────────────────────────────
    def conectar(self) -> bool:
        """
        Establece conexión a SQL Server usando SQLAlchemy y pyodbc.
        Habilita fast_executemany para rendimiento óptimo.
        """
        self.log.info("Conectando a SQL Server...")
        try:
            params = urllib.parse.quote_plus(
                f"DRIVER={{{self.driver}}};"
                f"SERVER={self.server};"
                f"DATABASE={self.database};"
                f"Trusted_Connection={self.trusted_connection};"
                "TrustServerCertificate=yes;"
            )
            conn_url = f"mssql+pyodbc:///?odbc_connect={params}"

            self.engine = create_engine(
                conn_url,
                fast_executemany=True,
                pool_pre_ping=True,
            )

            # Prueba de consulta
            with self.engine.connect() as conn:
                version = conn.execute(text("SELECT @@VERSION")).scalar()
                db_name = conn.execute(text("SELECT DB_NAME()")).scalar()
                self.log.info(f"Conexión exitosa a SQL Server!")
                self.log.info(f"  Versión: {version.splitlines()[0]}")
                self.log.info(f"  Base de datos activa: {db_name}")
            return True

        except Exception as exc:
            self.log.error(f"Error al conectar con SQL Server: {exc}")
            return False

    def desconectar(self) -> None:
        """Libera el pool de conexiones de SQLAlchemy."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.log.info("Conexión a SQL Server cerrada.")

    # ──────────────────────────────────────────────────────────
    # Creación de Tablas (DDL)
    # ──────────────────────────────────────────────────────────
    def crear_tablas(self) -> None:
        """
        Crea las tablas relacionales en SQL Server con Primary Keys,
        Foreign Keys e índices de optimización.
        """
        if self.engine is None:
            raise RuntimeError("No hay conexión activa a SQL Server. Llama primero a conectar().")

        self.log.info("Creando estructura de tablas en SQL Server...")

        ddl_script = """
        -- 1. Tabla Listings
        IF OBJECT_ID('dbo.calendar', 'U') IS NOT NULL DROP TABLE dbo.calendar;
        IF OBJECT_ID('dbo.reviews', 'U') IS NOT NULL DROP TABLE dbo.reviews;
        IF OBJECT_ID('dbo.calendar_summary', 'U') IS NOT NULL DROP TABLE dbo.calendar_summary;
        IF OBJECT_ID('dbo.listings', 'U') IS NOT NULL DROP TABLE dbo.listings;

        CREATE TABLE dbo.listings (
            id                          BIGINT PRIMARY KEY,
            name                        NVARCHAR(500),
            host_id                     BIGINT,
            host_name                   NVARCHAR(250),
            host_since                  DATE,
            host_is_superhost           BIT,
            neighbourhood_cleansed      NVARCHAR(150),
            latitude                    FLOAT,
            longitude                   FLOAT,
            property_type               NVARCHAR(100),
            room_type                   NVARCHAR(50),
            accommodates                INT,
            bathrooms                   FLOAT,
            bedrooms                    INT,
            beds                        INT,
            price                       FLOAT,
            price_category              NVARCHAR(50),
            minimum_nights              INT,
            maximum_nights              INT,
            availability_365            INT,
            number_of_reviews           INT,
            review_scores_rating        FLOAT,
            n_amenities                 INT,
            host_since_year             INT
        );

        -- 2. Tabla Reviews (con Foreign Key a Listings)
        CREATE TABLE dbo.reviews (
            id                          BIGINT PRIMARY KEY,
            listing_id                  BIGINT NOT NULL,
            date                        DATE,
            reviewer_id                 BIGINT,
            reviewer_name               NVARCHAR(250),
            comments                    NVARCHAR(MAX),
            year                        INT,
            month                       INT,
            day                         INT,
            quarter                     INT,
            is_weekend                  INT,
            CONSTRAINT FK_reviews_listings FOREIGN KEY (listing_id) REFERENCES dbo.listings(id)
        );

        -- 3. Tabla Calendar (con Foreign Key a Listings)
        CREATE TABLE dbo.calendar (
            listing_id                  BIGINT NOT NULL,
            date                        DATE NOT NULL,
            is_available                BIT,
            minimum_nights              INT,
            maximum_nights              INT,
            year                        INT,
            month                       INT,
            week                        INT,
            quarter                     INT,
            day_of_week                 INT,
            CONSTRAINT PK_calendar PRIMARY KEY (listing_id, date),
            CONSTRAINT FK_calendar_listings FOREIGN KEY (listing_id) REFERENCES dbo.listings(id)
        );

        -- 4. Tabla Calendar Summary (agregación mensual)
        CREATE TABLE dbo.calendar_summary (
            year                        INT NOT NULL,
            month                       INT NOT NULL,
            total_dias                  INT,
            dias_disponibles            INT,
            disponibilidad_pct          FLOAT,
            noches_minimas_prom         FLOAT,
            CONSTRAINT PK_calendar_summary PRIMARY KEY (year, month)
        );

        -- Índices para optimización de consultas
        CREATE NONCLUSTERED INDEX IX_listings_neighbourhood ON dbo.listings(neighbourhood_cleansed);
        CREATE NONCLUSTERED INDEX IX_listings_room_type ON dbo.listings(room_type);
        CREATE NONCLUSTERED INDEX IX_listings_price ON dbo.listings(price);
        CREATE NONCLUSTERED INDEX IX_reviews_listing_date ON dbo.reviews(listing_id, date);
        CREATE NONCLUSTERED INDEX IX_calendar_date ON dbo.calendar(date);
        """

        with self.engine.begin() as conn:
            conn.execute(text(ddl_script))

        self.log.info("Tablas e índices creados exitosamente en SQL Server.")

    # ──────────────────────────────────────────────────────────
    # Carga de Datos a SQL Server
    # ──────────────────────────────────────────────────────────
    def cargar_listings(self, df_listings: pd.DataFrame) -> int:
        """Carga el DataFrame de listings en la tabla dbo.listings."""
        cols_map = {
            "id": "id",
            "name": "name",
            "host_id": "host_id",
            "host_name": "host_name",
            "host_since": "host_since",
            "host_is_superhost": "host_is_superhost",
            "neighbourhood_cleansed": "neighbourhood_cleansed",
            "latitude": "latitude",
            "longitude": "longitude",
            "property_type": "property_type",
            "room_type": "room_type",
            "accommodates": "accommodates",
            "bathrooms": "bathrooms",
            "bedrooms": "bedrooms",
            "beds": "beds",
            "price_clean": "price",
            "price_category": "price_category",
            "minimum_nights": "minimum_nights",
            "maximum_nights": "maximum_nights",
            "availability_365": "availability_365",
            "number_of_reviews": "number_of_reviews",
            "review_scores_rating": "review_scores_rating",
            "n_amenities": "n_amenities",
            "host_since_year": "host_since_year",
        }

        cols_exist = [c for c in cols_map.keys() if c in df_listings.columns]
        df_sql = df_listings[cols_exist].rename(columns=cols_map).copy()

        # Ajuste de tipos
        if "host_is_superhost" in df_sql.columns:
            df_sql["host_is_superhost"] = df_sql["host_is_superhost"].fillna(False).astype(bool)

        self.log.info(f"[dbo.listings] Iniciando carga de {len(df_sql):,} registros...")
        t0 = datetime.now()

        df_sql.to_sql(
            "listings",
            con=self.engine,
            schema="dbo",
            if_exists="append",
            index=False,
            chunksize=2000,
        )

        elapsed = (datetime.now() - t0).total_seconds()
        self.log.info(f"[dbo.listings] Carga completada: {len(df_sql):,} registros insertados en {elapsed:.2f}s")
        return len(df_sql)

    def cargar_reviews(self, df_reviews: pd.DataFrame, batch_size: int = 25000) -> int:
        """Carga el DataFrame de reviews en la tabla dbo.reviews."""
        cols_needed = [
            "id", "listing_id", "date", "reviewer_id", "reviewer_name",
            "comments", "year", "month", "day", "quarter", "is_weekend"
        ]
        cols_exist = [c for c in cols_needed if c in df_reviews.columns]
        df_sql = df_reviews[cols_exist].copy()

        # Truncar comentarios muy largos para prevenir desbordes si aplica
        if "comments" in df_sql.columns:
            df_sql["comments"] = df_sql["comments"].astype(str).str.slice(0, 4000)

        total_filas = len(df_sql)
        self.log.info(f"[dbo.reviews] Iniciando carga de {total_filas:,} registros (por lotes de {batch_size:,})...")
        t0 = datetime.now()

        insertados = 0
        for i in range(0, total_filas, batch_size):
            chunk = df_sql.iloc[i : i + batch_size]
            chunk.to_sql(
                "reviews",
                con=self.engine,
                schema="dbo",
                if_exists="append",
                index=False,
                chunksize=2000,
            )
            insertados += len(chunk)
            self.log.info(f"  [dbo.reviews] Progreso: {insertados:,} / {total_filas:,} registros")

        elapsed = (datetime.now() - t0).total_seconds()
        self.log.info(f"[dbo.reviews] Carga completada: {insertados:,} registros en {elapsed:.2f}s")
        return insertados

    def cargar_calendar(self, df_calendar: pd.DataFrame, batch_size: int = 50000) -> int:
        """Carga el DataFrame de calendar en dbo.calendar."""
        cols_needed = [
            "listing_id", "date", "is_available", "minimum_nights",
            "maximum_nights", "year", "month", "week", "quarter", "day_of_week"
        ]
        cols_exist = [c for c in cols_needed if c in df_calendar.columns]
        df_sql = df_calendar[cols_exist].copy()

        if "is_available" in df_sql.columns:
            df_sql["is_available"] = df_sql["is_available"].fillna(False).astype(bool)

        total_filas = len(df_sql)
        self.log.info(f"[dbo.calendar] Iniciando carga de {total_filas:,} registros (por lotes de {batch_size:,})...")
        t0 = datetime.now()

        insertados = 0
        for i in range(0, total_filas, batch_size):
            chunk = df_sql.iloc[i : i + batch_size]
            chunk.to_sql(
                "calendar",
                con=self.engine,
                schema="dbo",
                if_exists="append",
                index=False,
                chunksize=5000,
            )
            insertados += len(chunk)
            self.log.info(f"  [dbo.calendar] Progreso: {insertados:,} / {total_filas:,} registros")

        elapsed = (datetime.now() - t0).total_seconds()
        self.log.info(f"[dbo.calendar] Carga completada: {insertados:,} registros en {elapsed:.2f}s")
        return insertados

    def cargar_calendar_summary(self, df_summary: pd.DataFrame) -> int:
        """Carga el resumen mensual de calendario en dbo.calendar_summary."""
        self.log.info(f"[dbo.calendar_summary] Insertando {len(df_summary)} registros...")
        df_summary.to_sql(
            "calendar_summary",
            con=self.engine,
            schema="dbo",
            if_exists="append",
            index=False,
        )
        self.log.info("[dbo.calendar_summary] Carga completada.")
        return len(df_summary)

    def cargar_todo(self, dataframes: Dict[str, pd.DataFrame]) -> Dict[str, int]:
        """Ejecuta la creación de esquema y la carga completa de todas las tablas."""
        self.crear_tablas()

        resultados = {}
        if "listings" in dataframes:
            resultados["listings"] = self.cargar_listings(dataframes["listings"])

        if "reviews" in dataframes:
            resultados["reviews"] = self.cargar_reviews(dataframes["reviews"])

        if "calendar" in dataframes:
            resultados["calendar"] = self.cargar_calendar(dataframes["calendar"])

        if "calendar_summary" in dataframes:
            resultados["calendar_summary"] = self.cargar_calendar_summary(dataframes["calendar_summary"])

        self.log.info("=" * 60)
        self.log.info(" RESUMEN GENERAL DE CARGA EN SQL SERVER")
        self.log.info("=" * 60)
        for tbl, count in resultados.items():
            self.log.info(f"  ✓ dbo.{tbl:<18}: {count:>10,} registros")
        self.log.info("=" * 60)

        return resultados

    # ──────────────────────────────────────────────────────────
    # Exportación a Excel (.xlsx)
    # ──────────────────────────────────────────────────────────
    def exportar_excel(self, dataframes: Dict[str, pd.DataFrame]) -> None:
        """
        Exporta los datos procesados a archivos XLSX estructurados:
        1. listings_procesados.xlsx (tabla de alojamientos limpia)
        2. calendar_resumen_ejecutivo.xlsx (resumen de ocupación mensual y métricas)
        3. airbnb_dashboard_datos.xlsx (libro maestro multi-hoja para visualización en Power BI)
        """
        self.log.info("Exportando conjuntos de datos a archivos XLSX en data/exports/...")

        # 1. Listings XLSX
        if "listings" in dataframes:
            cols_export_l = [
                "id", "name", "host_id", "host_name", "host_since", "host_is_superhost",
                "neighbourhood_cleansed", "room_type", "accommodates", "bedrooms",
                "price_clean", "price_category", "minimum_nights", "availability_365",
                "number_of_reviews", "review_scores_rating", "n_amenities"
            ]
            cols_l = [c for c in cols_export_l if c in dataframes["listings"].columns]
            out_l = EXPORTS_DIR / "listings_procesados.xlsx"
            dataframes["listings"][cols_l].to_excel(out_l, index=False, sheet_name="Listings")
            self.log.info(f"  ✓ Archivo generado: {out_l.name} ({out_l.stat().st_size / (1024*1024):.2f} MB)")

        # 2. Calendar Resumen Ejecutivo XLSX
        if "calendar_summary" in dataframes:
            out_cs = EXPORTS_DIR / "calendar_resumen_ejecutivo.xlsx"
            dataframes["calendar_summary"].to_excel(out_cs, index=False, sheet_name="Resumen_Mensual")
            self.log.info(f"  ✓ Archivo generado: {out_cs.name}")

        # 3. Libro Maestro Multi-Pestaña para Análisis / Power BI
        master_file = EXPORTS_DIR / "airbnb_reporte_ejecutivo.xlsx"
        with pd.ExcelWriter(master_file, engine="openpyxl") as writer:
            if "listings" in dataframes:
                dataframes["listings"][cols_l].head(10000).to_excel(writer, index=False, sheet_name="Listings_Sample")
            if "calendar_summary" in dataframes:
                dataframes["calendar_summary"].to_excel(writer, index=False, sheet_name="Ocupacion_Mensual")
            if "reviews" in dataframes:
                cols_r = ["id", "listing_id", "date", "reviewer_name", "year", "month", "quarter", "is_weekend"]
                cols_rev_exist = [c for c in cols_r if c in dataframes["reviews"].columns]
                dataframes["reviews"][cols_rev_exist].head(15000).to_excel(writer, index=False, sheet_name="Reviews_Sample")

        self.log.info(f"  ✓ Libro maestro multi-hoja generado: {master_file.name}")


# ══════════════════════════════════════════════════════════════
# Ejecución directa para pruebas del script
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("\n" + "=" * 60)
    print(" PRUEBA DE LA CLASE Carga")
    print("=" * 60)

    loader = Carga()
    if not loader.conectar():
        print("❌ Error de conexión a SQL Server.")
        sys.exit(1)

    print("\n--- Cargando datos procesados desde data/processed/ ---")
    df_l = pd.read_csv(PROCESSED_DIR / "listings_clean.csv", nrows=1000)
    df_r = pd.read_csv(PROCESSED_DIR / "reviews_clean.csv", nrows=2000)
    df_c = pd.read_csv(PROCESSED_DIR / "calendar_clean.csv", nrows=5000)
    df_s = pd.read_csv(PROCESSED_DIR / "calendar_summary_clean.csv")

    # Filtrar reviews y calendar para que sólo tengan IDs existentes en df_l (para respetar FK en muestra)
    valid_ids = set(df_l["id"])
    df_r = df_r[df_r["listing_id"].isin(valid_ids)]
    df_c = df_c[df_c["listing_id"].isin(valid_ids)]

    print(f"Muestras preparadas: listings={len(df_l)}, reviews={len(df_r)}, calendar={len(df_c)}")

    res = loader.cargar_todo({
        "listings": df_l,
        "reviews": df_r,
        "calendar": df_c,
        "calendar_summary": df_s,
    })

    print("\n--- Generando archivos XLSX ---")
    loader.exportar_excel({
        "listings": df_l,
        "reviews": df_r,
        "calendar_summary": df_s,
    })

    loader.desconectar()
    print("\n" + "=" * 60)
    print(" Clase Carga - FUNCIONAL")
    print("=" * 60)
