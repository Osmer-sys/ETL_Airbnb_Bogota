"""
================================================================
 Taller Evaluativo 2 - Proceso ETL con datasets de Airbnb
 Archivo  : extraccion.py
 Clase    : Extraccion
 Autor    : Pipeline ETL Airbnb Bogota
 Fecha    : 2026-10-03
================================================================
Descripcion:
    Modulo de extraccion del pipeline ETL.
    Implementa la clase Extraccion que establece conexion con
    MongoDB, consulta las colecciones Listings, Reviews y
    Calendar, carga los datos en DataFrames de pandas y
    registra cada operacion en un archivo de log.

Uso:
    from scripts.extraccion import Extraccion

    ext = Extraccion()
    ext.conectar()
    dataframes = ext.extraer_todo()
    df_listings  = dataframes["listings"]
    df_reviews   = dataframes["reviews"]
    df_calendar  = dataframes["calendar"]
================================================================
"""

import os
import sys
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import pymongo
from pymongo import MongoClient
from pymongo.database import Database
from dotenv import load_dotenv


# ── Rutas base ────────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(exist_ok=True)


# ── Configuracion de logging del modulo ───────────────────────
def _crear_logger(nombre: str, log_file: Path) -> logging.Logger:
    """
    Crea y retorna un logger con handler de archivo (UTF-8)
    y handler de consola, sin duplicar handlers si se
    instancia la clase varias veces.
    """
    logger = logging.getLogger(nombre)
    if logger.handlers:
        return logger

    logger.setLevel(logging.INFO)
    fmt = logging.Formatter(
        fmt="%(asctime)s | %(levelname)-8s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Handler archivo UTF-8
    fh = logging.FileHandler(log_file, encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    # Handler consola: compatible con terminal Y con Jupyter (IPyKernel)
    try:
        import io as _io
        _stream = open(sys.stdout.fileno(), mode="w",
                       encoding="utf-8", buffering=1, errors="replace")
    except (AttributeError, _io.UnsupportedOperation):
        # En Jupyter, sys.stdout es un OutStream sin fileno() real
        _stream = sys.stdout
    ch = logging.StreamHandler(_stream)
    ch.setFormatter(fmt)
    logger.addHandler(ch)

    return logger


# ═══════════════════════════════════════════════════════════════
class Extraccion:
    """
    Clase responsable de la capa de Extraccion (E) del pipeline ETL.

    Responsabilidades:
    ------------------
    - Establecer y gestionar la conexion con MongoDB local.
    - Consultar las colecciones: Listings, Reviews, Calendar.
    - Cargar los datos de cada coleccion en DataFrames de pandas.
    - Registrar en log: conexion realizada, tiempos y cantidad
      de registros extraidos por coleccion.

    Atributos:
    ----------
    mongo_uri : str
        URI de conexion a MongoDB.
    db_name : str
        Nombre de la base de datos MongoDB.
    client : MongoClient | None
        Cliente activo de pymongo (None si no conectado).
    db : Database | None
        Referencia a la base de datos activa.
    log : logging.Logger
        Logger dedicado a esta instancia.

    Ejemplo de uso:
    ---------------
    >>> ext = Extraccion()
    >>> ext.conectar()
    >>> dfs = ext.extraer_todo()
    >>> print(dfs["listings"].shape)
    >>> ext.desconectar()
    """

    # Nombres canonicos de las colecciones MongoDB
    COLECCIONES = ["listings", "reviews", "calendar"]

    def __init__(
        self,
        mongo_uri: Optional[str] = None,
        db_name:   Optional[str] = None,
    ):
        """
        Inicializa la instancia de Extraccion.

        Parametros:
        -----------
        mongo_uri : str, opcional
            URI de conexion. Si None, lee MONGO_URI del .env
            o usa 'mongodb://localhost:27017/' por defecto.
        db_name : str, opcional
            Nombre de la base de datos. Si None, lee MONGO_DB
            del .env o usa 'airbnb_bogota' por defecto.
        """
        self.mongo_uri = mongo_uri or os.getenv(
            "MONGO_URI", "mongodb://localhost:27017/"
        )
        self.db_name = db_name or os.getenv("MONGO_DB", "airbnb_bogota")

        self.client: Optional[MongoClient] = None
        self.db:     Optional[Database]    = None

        # Logger con timestamp en nombre de archivo para no
        # mezclar ejecuciones distintas
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file  = LOGS_DIR / f"extraccion_{timestamp}.log"
        self.log  = _crear_logger(f"Extraccion_{timestamp}", log_file)

        self.log.info("=" * 60)
        self.log.info(" CLASE EXTRACCION - Inicializada")
        self.log.info(f" MongoDB URI : {self.mongo_uri}")
        self.log.info(f" Base datos  : {self.db_name}")
        self.log.info(f" Log archivo : {log_file.name}")
        self.log.info("=" * 60)

    # ──────────────────────────────────────────────────────────
    def conectar(self, timeout_ms: int = 5000) -> bool:
        """
        Establece la conexion con el servidor MongoDB.

        Parametros:
        -----------
        timeout_ms : int
            Tiempo maximo de espera en milisegundos (default 5000).

        Retorna:
        --------
        bool : True si la conexion fue exitosa, False en caso contrario.

        Raises:
        -------
        ConnectionError : Si no se puede establecer la conexion.
        """
        self.log.info("Estableciendo conexion con MongoDB...")
        try:
            self.client = MongoClient(
                self.mongo_uri,
                serverSelectionTimeoutMS=timeout_ms,
            )
            # Verificar conexion activa con ping
            ping = self.client.admin.command("ping")
            self.db = self.client[self.db_name]

            self.log.info(f"Conexion exitosa - ping: {ping}")
            self.log.info(f"Base de datos activa: '{self.db_name}'")

            # Verificar que las colecciones requeridas existan
            existentes = self.db.list_collection_names()
            for col in self.COLECCIONES:
                if col in existentes:
                    count = self.db[col].count_documents({})
                    self.log.info(
                        f"  Coleccion '{col}' disponible: {count:,} documentos"
                    )
                else:
                    self.log.warning(
                        f"  Coleccion '{col}' NO encontrada en '{self.db_name}'"
                    )
            return True

        except pymongo.errors.ServerSelectionTimeoutError as exc:
            self.log.error(f"Timeout al conectar a MongoDB: {exc}")
            return False
        except Exception as exc:
            self.log.error(f"Error inesperado al conectar: {exc}")
            return False

    # ──────────────────────────────────────────────────────────
    def desconectar(self) -> None:
        """
        Cierra la conexion con MongoDB y libera recursos.
        """
        if self.client:
            self.client.close()
            self.client = None
            self.db     = None
            self.log.info("Conexion con MongoDB cerrada correctamente.")

    # ──────────────────────────────────────────────────────────
    def _verificar_conexion(self) -> None:
        """
        Valida que exista una conexion activa.

        Raises:
        -------
        RuntimeError : Si no hay conexion establecida.
        """
        if self.client is None or self.db is None:
            raise RuntimeError(
                "No hay conexion activa con MongoDB. "
                "Llama primero a extraccion.conectar()."
            )

    # ──────────────────────────────────────────────────────────
    def extraer_coleccion(
        self,
        nombre_coleccion: str,
        filtro:    Optional[dict] = None,
        proyeccion: Optional[dict] = None,
        limite:    Optional[int]  = None,
    ) -> pd.DataFrame:
        """
        Consulta una coleccion MongoDB y retorna un DataFrame.

        Parametros:
        -----------
        nombre_coleccion : str
            Nombre de la coleccion a consultar.
        filtro : dict, opcional
            Filtro MongoDB (equivalente al WHERE en SQL).
            Ejemplo: {"room_type": "Private room"}
        proyeccion : dict, opcional
            Campos a incluir/excluir.
            Ejemplo: {"_id": 0, "id": 1, "price": 1}
        limite : int, opcional
            Maximo de documentos a retornar. None = todos.

        Retorna:
        --------
        pd.DataFrame con los documentos de la coleccion.

        Raises:
        -------
        RuntimeError : Si no hay conexion activa.
        ValueError   : Si la coleccion no existe.
        """
        self._verificar_conexion()

        # Validar existencia de la coleccion
        if nombre_coleccion not in self.db.list_collection_names():
            raise ValueError(
                f"La coleccion '{nombre_coleccion}' no existe "
                f"en la base de datos '{self.db_name}'."
            )

        self.log.info(f"[{nombre_coleccion}] Iniciando extraccion...")

        col    = self.db[nombre_coleccion]
        filtro = filtro     or {}
        proj   = proyeccion or {"_id": 0}   # excluir _id por defecto

        import time
        t0 = time.time()

        # Ejecutar consulta
        cursor = col.find(filtro, proj)
        if limite:
            cursor = cursor.limit(limite)

        # Cargar en DataFrame
        df = pd.DataFrame(list(cursor))
        elapsed = time.time() - t0

        n_filas   = len(df)
        n_cols    = len(df.columns) if not df.empty else 0
        mem_mb    = df.memory_usage(deep=True).sum() / (1024 ** 2)

        self.log.info(
            f"[{nombre_coleccion}] Extraccion completada: "
            f"{n_filas:,} registros | {n_cols} campos | "
            f"{mem_mb:.1f} MB | {elapsed:.2f}s"
        )

        if df.empty:
            self.log.warning(
                f"[{nombre_coleccion}] DataFrame vacio. "
                "Verifica el filtro o el contenido de la coleccion."
            )

        return df

    # ──────────────────────────────────────────────────────────
    def extraer_listings(
        self,
        filtro:     Optional[dict] = None,
        proyeccion: Optional[dict] = None,
        limite:     Optional[int]  = None,
    ) -> pd.DataFrame:
        """
        Extrae la coleccion 'listings' completa o filtrada.

        Parametros:
        -----------
        filtro : dict, opcional
            Filtro MongoDB. Ej: {"room_type": "Entire home/apt"}
        proyeccion : dict, opcional
            Campos especificos a extraer.
        limite : int, opcional
            Numero maximo de registros.

        Retorna:
        --------
        pd.DataFrame con los alojamientos de Airbnb Bogota.
        """
        return self.extraer_coleccion("listings", filtro, proyeccion, limite)

    # ──────────────────────────────────────────────────────────
    def extraer_reviews(
        self,
        filtro:     Optional[dict] = None,
        proyeccion: Optional[dict] = None,
        limite:     Optional[int]  = None,
    ) -> pd.DataFrame:
        """
        Extrae la coleccion 'reviews' completa o filtrada.

        Parametros:
        -----------
        filtro : dict, opcional
            Filtro MongoDB. Ej: {"date": {"$gte": "2023-01-01"}}
        proyeccion : dict, opcional
            Campos especificos a extraer.
        limite : int, opcional
            Numero maximo de registros.

        Retorna:
        --------
        pd.DataFrame con las resenas de Airbnb Bogota.
        """
        return self.extraer_coleccion("reviews", filtro, proyeccion, limite)

    # ──────────────────────────────────────────────────────────
    def extraer_calendar(
        self,
        filtro:     Optional[dict] = None,
        proyeccion: Optional[dict] = None,
        limite:     Optional[int]  = None,
    ) -> pd.DataFrame:
        """
        Extrae la coleccion 'calendar' completa o filtrada.

        Parametros:
        -----------
        filtro : dict, opcional
            Filtro MongoDB. Ej: {"available": "t"}
        proyeccion : dict, opcional
            Campos especificos a extraer.
        limite : int, opcional
            Numero maximo de registros. Se recomienda usar limite
            para esta coleccion (7M documentos).

        Retorna:
        --------
        pd.DataFrame con el calendario de disponibilidad.
        """
        return self.extraer_coleccion("calendar", filtro, proyeccion, limite)

    # ──────────────────────────────────────────────────────────
    def extraer_todo(self) -> dict[str, pd.DataFrame]:
        """
        Extrae las tres colecciones completas del pipeline ETL:
        Listings, Reviews y Calendar.

        Retorna:
        --------
        dict con tres DataFrames:
            {
                "listings" : pd.DataFrame,
                "reviews"  : pd.DataFrame,
                "calendar" : pd.DataFrame,
            }

        Nota:
        -----
        La coleccion 'calendar' contiene ~7M documentos.
        Asegurate de tener suficiente RAM disponible (~2-4 GB).
        Para pruebas usa extraer_calendar(limite=100000).

        Raises:
        -------
        RuntimeError : Si no hay conexion activa.
        """
        self._verificar_conexion()

        self.log.info("-" * 60)
        self.log.info("Extraccion completa de las 3 colecciones...")
        self.log.info("-" * 60)

        import time
        t_inicio = time.time()

        dataframes = {
            "listings": self.extraer_listings(),
            "reviews":  self.extraer_reviews(),
            "calendar": self.extraer_calendar(),
        }

        t_total = time.time() - t_inicio

        self.log.info("=" * 60)
        self.log.info(" RESUMEN DE EXTRACCION")
        self.log.info("=" * 60)
        total_registros = 0
        for nombre, df in dataframes.items():
            n = len(df)
            total_registros += n
            mem = df.memory_usage(deep=True).sum() / (1024 ** 2)
            self.log.info(
                f"  {nombre:<12}: {n:>10,} registros | "
                f"{len(df.columns):>3} campos | {mem:>7.1f} MB"
            )
        self.log.info(f"  {'TOTAL':<12}: {total_registros:>10,} registros")
        self.log.info(f"  Tiempo total extraccion: {t_total:.1f}s")
        self.log.info("=" * 60)

        return dataframes

    # ──────────────────────────────────────────────────────────
    def resumen_coleccion(self, nombre_coleccion: str) -> dict:
        """
        Retorna un diccionario con estadisticas basicas
        de una coleccion sin cargar todos los documentos.

        Parametros:
        -----------
        nombre_coleccion : str
            Nombre de la coleccion a inspeccionar.

        Retorna:
        --------
        dict con: nombre, total_documentos, campos, muestra.
        """
        self._verificar_conexion()
        col   = self.db[nombre_coleccion]
        count = col.count_documents({})
        muestra = col.find_one({}, {"_id": 0}) or {}
        campos  = list(muestra.keys())

        resumen = {
            "coleccion":         nombre_coleccion,
            "total_documentos":  count,
            "n_campos":          len(campos),
            "campos":            campos,
        }

        self.log.info(
            f"Resumen '{nombre_coleccion}': "
            f"{count:,} docs | {len(campos)} campos"
        )
        return resumen

    # ──────────────────────────────────────────────────────────
    def __repr__(self) -> str:
        estado = "conectada" if self.client else "desconectada"
        return (
            f"Extraccion("
            f"db='{self.db_name}', "
            f"uri='{self.mongo_uri}', "
            f"estado='{estado}')"
        )

    # ──────────────────────────────────────────────────────────
    def __enter__(self):
        """Soporte para uso como context manager (with)."""
        self.conectar()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Cierra conexion al salir del bloque with."""
        self.desconectar()
        return False


# ══════════════════════════════════════════════════════════════
# Ejecucion directa para prueba rapida
# ══════════════════════════════════════════════════════════════
if __name__ == "__main__":

    print("\n" + "=" * 60)
    print(" PRUEBA DE LA CLASE Extraccion")
    print("=" * 60)

    # Instanciar y conectar
    ext = Extraccion()
    ok  = ext.conectar()

    if not ok:
        print("ERROR: No se pudo conectar a MongoDB.")
        sys.exit(1)

    # Resumen de colecciones sin cargar todo en RAM
    print("\n--- Resumen de colecciones ---")
    for col in Extraccion.COLECCIONES:
        r = ext.resumen_coleccion(col)
        print(f"  {r['coleccion']:<12}: {r['total_documentos']:>10,} docs | "
              f"{r['n_campos']} campos")

    # Extraccion de muestra (100 registros de cada coleccion)
    print("\n--- Extraccion de muestra (100 registros c/u) ---")
    df_list = ext.extraer_listings(limite=100)
    df_rev  = ext.extraer_reviews(limite=100)
    df_cal  = ext.extraer_calendar(limite=100)

    print(f"\n  listings  shape : {df_list.shape}")
    print(f"  reviews   shape : {df_rev.shape}")
    print(f"  calendar  shape : {df_cal.shape}")

    print("\n  Columnas listings :")
    print(f"  {list(df_list.columns[:8])}...")

    print("\n  Primeras 3 filas de reviews:")
    print(df_rev[["listing_id", "date", "reviewer_name", "comments"]]
          .head(3).to_string(index=False))

    # Desconectar
    ext.desconectar()

    print("\n" + "=" * 60)
    print(" Clase Extraccion - FUNCIONAL")
    print("=" * 60)
