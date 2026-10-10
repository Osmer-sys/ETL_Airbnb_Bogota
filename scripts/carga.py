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
    1. Establece conexión a SQLite (archivo: data/airbnb_bogota.db).
    2. Crea las tablas relacionales con tipos de datos adecuados,
       claves primarias (PK), claves foráneas (FK) e índices.
    3. Carga los DataFrames limpios en las tablas de SQLite
       utilizando inserción por lotes.
    4. Exporta los conjuntos de datos limpios a archivos XLSX estructurados.
    5. Registra en un archivo de log cada paso del proceso, número
       de registros insertados por tabla, tiempos y posibles alertas.
================================================================
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional, Dict

import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text, event
from sqlalchemy.engine import Engine
from dotenv import load_dotenv

# ── Rutas base ────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LOGS_DIR = BASE_DIR / "logs"
EXPORTS_DIR = BASE_DIR / "data" / "exports"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
DEFAULT_SQLITE_PATH = BASE_DIR / "data" / "airbnb_bogota.db"

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
    - Gestionar la conexión a SQLite mediante SQLAlchemy.
    - Definir y ejecutar el esquema DDL relacional (tablas, PK, FK, índices).
    - Cargar DataFrames limpios a SQLite por lotes.
    - Exportar datos analíticos a archivos Excel (.xlsx).
    - Registrar métricas de inserción en log por cada ejecución.
    """

    def __init__(
        self,
        sqlite_path: Optional[str | Path] = None,
        **kwargs,
    ):
        """Inicializa la clase Carga con la ruta del archivo SQLite."""
        raw_path = sqlite_path or os.getenv("SQLITE_PATH", str(DEFAULT_SQLITE_PATH))
        self.sqlite_path = Path(raw_path)
        if not self.sqlite_path.is_absolute():
            self.sqlite_path = BASE_DIR / self.sqlite_path
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)

        self.engine: Optional[Engine] = None

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_file = LOGS_DIR / f"carga_sqlite_{timestamp}.log"
        self.log = _crear_logger(f"Carga_{timestamp}", self.log_file)

        self.log.info("=" * 60)
        self.log.info(" CLASE CARGA - Inicializada")
        self.log.info(f" Motor       : SQLite")
        self.log.info(f" Archivo DB  : {self.sqlite_path}")
        self.log.info(f" Archivo Log : {self.log_file.name}")
        self.log.info("=" * 60)

    # ──────────────────────────────────────────────────────────
    # Conexión a SQLite
    # ──────────────────────────────────────────────────────────
    def conectar(self) -> bool:
        """
        Establece conexión a SQLite usando SQLAlchemy.
        Activa el enforcement de claves foráneas (PRAGMA foreign_keys=ON).
        """
        self.log.info("Conectando a SQLite...")
        try:
            self.engine = create_engine(
                f"sqlite:///{self.sqlite_path}",
                pool_pre_ping=True,
            )

            @event.listens_for(self.engine, "connect")
            def _fk_pragma(dbapi_connection, connection_record):
                cursor = dbapi_connection.cursor()
                cursor.execute("PRAGMA foreign_keys=ON")
                cursor.close()

            # Prueba de consulta
            with self.engine.connect() as conn:
                version = conn.execute(text("SELECT sqlite_version()")).scalar()
                conn.execute(text("PRAGMA foreign_keys=ON"))
                self.log.info("Conexión exitosa a SQLite!")
                self.log.info(f"  Versión: {version}")
                self.log.info(f"  Archivo activo: {self.sqlite_path.name}")
            return True

        except Exception as exc:
            self.log.error(f"Error al conectar con SQLite: {exc}")
            return False

    def desconectar(self) -> None:
        """Libera el pool de conexiones de SQLAlchemy."""
        if self.engine:
            self.engine.dispose()
            self.engine = None
            self.log.info("Conexión a SQLite cerrada.")

    # ──────────────────────────────────────────────────────────
    # Creación de Tablas (DDL)
    # ──────────────────────────────────────────────────────────
    def crear_tablas(self) -> None:
        """
        Crea las tablas relacionales en SQLite con Primary Keys,
        Foreign Keys e índices de optimización.
        """
        if self.engine is None:
            raise RuntimeError("No hay conexión activa a SQLite. Llama primero a conectar().")

        self.log.info("Creando estructura de tablas en SQLite...")

        ddl_statements = [
            "DROP TABLE IF EXISTS calendar;",
            "DROP TABLE IF EXISTS reviews;",
            "DROP TABLE IF EXISTS calendar_summary;",
            "DROP TABLE IF EXISTS listings;",

            """
            CREATE TABLE listings (
                id                          INTEGER PRIMARY KEY,
                name                        TEXT,
                host_id                     INTEGER,
                host_name                   TEXT,
                host_since                  TEXT,
                host_is_superhost           INTEGER,
                neighbourhood_cleansed      TEXT,
                latitude                    REAL,
                longitude                   REAL,
                property_type               TEXT,
                room_type                   TEXT,
                accommodates                INTEGER,
                bathrooms                   REAL,
                bedrooms                    INTEGER,
                beds                        INTEGER,
                price                       REAL,
                price_category              TEXT,
                minimum_nights              INTEGER,
                maximum_nights              INTEGER,
                availability_365            INTEGER,
                number_of_reviews           INTEGER,
                review_scores_rating        REAL,
                n_amenities                 INTEGER,
                host_since_year             INTEGER
            )
            """,

            """
            CREATE TABLE reviews (
                id                          INTEGER PRIMARY KEY,
                listing_id                  INTEGER NOT NULL,
                date                        TEXT,
                reviewer_id                 INTEGER,
                reviewer_name               TEXT,
                comments                    TEXT,
                year                        INTEGER,
                month                       INTEGER,
                day                         INTEGER,
                quarter                     INTEGER,
                is_weekend                  INTEGER,
                CONSTRAINT FK_reviews_listings FOREIGN KEY (listing_id) REFERENCES listings(id)
            )
            """,

            """
            CREATE TABLE calendar (
                listing_id                  INTEGER NOT NULL,
                date                        TEXT NOT NULL,
                is_available                INTEGER,
                minimum_nights              INTEGER,
                maximum_nights              INTEGER,
                year                        INTEGER,
                month                       INTEGER,
                week                        INTEGER,
                quarter                     INTEGER,
                day_of_week                 INTEGER,
                CONSTRAINT PK_calendar PRIMARY KEY (listing_id, date),
                CONSTRAINT FK_calendar_listings FOREIGN KEY (listing_id) REFERENCES listings(id)
            )
            """,

            """
            CREATE TABLE calendar_summary (
                year                        INTEGER NOT NULL,
                month                       INTEGER NOT NULL,
                total_dias                  INTEGER,
                dias_disponibles            INTEGER,
                disponibilidad_pct          REAL,
                noches_minimas_prom         REAL,
                CONSTRAINT PK_calendar_summary PRIMARY KEY (year, month)
            )
            """,

            "CREATE INDEX IF NOT EXISTS IX_listings_neighbourhood ON listings(neighbourhood_cleansed);",
            "CREATE INDEX IF NOT EXISTS IX_listings_room_type ON listings(room_type);",
            "CREATE INDEX IF NOT EXISTS IX_listings_price ON listings(price);",
            "CREATE INDEX IF NOT EXISTS IX_reviews_listing_date ON reviews(listing_id, date);",
            "CREATE INDEX IF NOT EXISTS IX_calendar_date ON calendar(date);",
        ]

        with self.engine.begin() as conn:
            conn.execute(text("PRAGMA foreign_keys=ON"))
            for stmt in ddl_statements:
                conn.execute(text(stmt))

        self.log.info("Tablas e índices creados exitosamente en SQLite.")

    # ──────────────────────────────────────────────────────────
    # Carga de Datos a SQLite
    # ──────────────────────────────────────────────────────────
    def cargar_listings(self, df_listings: pd.DataFrame) -> int:
        """Carga el DataFrame de listings en la tabla listings."""
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

        # Ajuste de tipos (SQLite guarda booleanos como 0/1)
        if "host_is_superhost" in df_sql.columns:
            df_sql["host_is_superhost"] = df_sql["host_is_superhost"].fillna(False).astype(bool).astype(int)

        self.log.info(f"[listings] Iniciando carga de {len(df_sql):,} registros...")
        t0 = datetime.now()

        df_sql.to_sql(
            "listings",
            con=self.engine,
            if_exists="append",
            index=False,
            chunksize=2000,
        )

        elapsed = (datetime.now() - t0).total_seconds()
        self.log.info(f"[listings] Carga completada: {len(df_sql):,} registros insertados en {elapsed:.2f}s")
        return len(df_sql)

    def cargar_reviews(self, df_reviews: pd.DataFrame, batch_size: int = 25000) -> int:
        """Carga el DataFrame de reviews en la tabla reviews."""
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
        self.log.info(f"[reviews] Iniciando carga de {total_filas:,} registros (por lotes de {batch_size:,})...")
        t0 = datetime.now()

        insertados = 0
        for i in range(0, total_filas, batch_size):
            chunk = df_sql.iloc[i : i + batch_size]
            chunk.to_sql(
                "reviews",
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=500,
            )
            insertados += len(chunk)
            self.log.info(f"  [reviews] Progreso: {insertados:,} / {total_filas:,} registros")

        elapsed = (datetime.now() - t0).total_seconds()
        self.log.info(f"[reviews] Carga completada: {insertados:,} registros en {elapsed:.2f}s")
        return insertados

    def cargar_calendar(self, df_calendar: pd.DataFrame, batch_size: int = 50000) -> int:
        """Carga el DataFrame de calendar en calendar."""
        cols_needed = [
            "listing_id", "date", "is_available", "minimum_nights",
            "maximum_nights", "year", "month", "week", "quarter", "day_of_week"
        ]
        cols_exist = [c for c in cols_needed if c in df_calendar.columns]
        df_sql = df_calendar[cols_exist].copy()

        if "is_available" in df_sql.columns:
            df_sql["is_available"] = df_sql["is_available"].fillna(False).astype(bool).astype(int)

        total_filas = len(df_sql)
        self.log.info(f"[calendar] Iniciando carga de {total_filas:,} registros (por lotes de {batch_size:,})...")
        t0 = datetime.now()

        insertados = 0
        for i in range(0, total_filas, batch_size):
            chunk = df_sql.iloc[i : i + batch_size]
            chunk.to_sql(
                "calendar",
                con=self.engine,
                if_exists="append",
                index=False,
                chunksize=500,
            )
            insertados += len(chunk)
            self.log.info(f"  [calendar] Progreso: {insertados:,} / {total_filas:,} registros")

        elapsed = (datetime.now() - t0).total_seconds()
        self.log.info(f"[calendar] Carga completada: {insertados:,} registros en {elapsed:.2f}s")
        return insertados

    def cargar_calendar_summary(self, df_summary: pd.DataFrame) -> int:
        """Carga el resumen mensual de calendario en calendar_summary."""
        self.log.info(f"[calendar_summary] Insertando {len(df_summary)} registros...")
        df_summary.to_sql(
            "calendar_summary",
            con=self.engine,
            if_exists="append",
            index=False,
        )
        self.log.info("[calendar_summary] Carga completada.")
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
        self.log.info(" RESUMEN GENERAL DE CARGA EN SQLITE")
        self.log.info("=" * 60)
        for tbl, count in resultados.items():
            self.log.info(f"  ✓ {tbl:<18}: {count:>10,} registros")
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
        print("❌ Error de conexión a SQLite.")
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
