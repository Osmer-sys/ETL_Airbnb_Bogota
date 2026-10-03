"""
=============================================================
 Taller Evaluativo 2 - Fase 1: Ingesta CSV.GZ -> MongoDB
 Script : 01_ingest_to_mongodb.py
 Autor  : Pipeline ETL Airbnb Bogota
 Fecha  : 2026-10-03
=============================================================
Descripcion:
    Carga los datasets Airbnb de Bogota (listings, calendar,
    reviews) desde archivos .csv.gz a colecciones MongoDB.

    Como mongoimport no soporta --gzip en esta version,
    descomprimimos en memoria con Python y usamos mongoimport
    via stdin (pipe), o alternativamente usamos pymongo
    directamente con pandas para la insercion en lotes.

    Flujo:
      1. Validar existencia de archivos fuente
      2. Verificar conectividad con MongoDB
      3. Drop + recrear cada coleccion (idempotente)
      4. Leer CSV.GZ con pandas, insertar por lotes via pymongo
      5. Verificar conteos post-carga
      6. Crear indices basicos
      7. Loguear cada paso en logs/
=============================================================
"""

import os
import sys
import logging
import time
import math
from datetime import datetime
from pathlib import Path

import pandas as pd
import pymongo
from tqdm import tqdm
from dotenv import load_dotenv

# ─────────────────────────────────────────────
# 0. Configuracion de rutas y entorno
# ─────────────────────────────────────────────
BASE_DIR = Path(__file__).resolve().parent.parent
load_dotenv(BASE_DIR / ".env")

LOGS_DIR = BASE_DIR / "logs"
RAW_DIR  = BASE_DIR / "data" / "raw"
LOGS_DIR.mkdir(exist_ok=True)

TIMESTAMP = datetime.now().strftime("%Y%m%d_%H%M%S")
LOG_FILE  = LOGS_DIR / f"fase1_ingesta_{TIMESTAMP}.log"

MONGO_URI = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
MONGO_DB  = os.getenv("MONGO_DB",  "airbnb_bogota")

CHUNK_SIZE = 5000   # documentos por lote de insercion

# ─────────────────────────────────────────────
# 1. Configurar logging (archivo UTF-8 + consola ASCII)
# ─────────────────────────────────────────────
fmt = logging.Formatter(
    fmt="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

# Handler de archivo: siempre UTF-8
fh = logging.FileHandler(LOG_FILE, encoding="utf-8")
fh.setFormatter(fmt)

# Handler de consola: reemplaza caracteres no imprimibles
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(fmt)
ch.stream = open(sys.stdout.fileno(), mode='w',
                 encoding='utf-8', buffering=1, errors='replace')

logging.basicConfig(level=logging.INFO, handlers=[fh, ch])
log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# 2. Configuracion de colecciones
# ─────────────────────────────────────────────
COLLECTIONS = {
    "listings": {
        "file": RAW_DIR / "listings.csv.gz",
        "collection": "listings",
        "indexes": [
            [("id", pymongo.ASCENDING)],
            [("host_id", pymongo.ASCENDING)],
            [("neighbourhood_cleansed", pymongo.ASCENDING)],
            [("room_type", pymongo.ASCENDING)],
            [("latitude", pymongo.ASCENDING), ("longitude", pymongo.ASCENDING)],
        ],
    },
    "calendar": {
        "file": RAW_DIR / "calendar.csv.gz",
        "collection": "calendar",
        "indexes": [
            [("listing_id", pymongo.ASCENDING)],
            [("date", pymongo.ASCENDING)],
            [("listing_id", pymongo.ASCENDING), ("date", pymongo.ASCENDING)],
        ],
    },
    "reviews": {
        "file": RAW_DIR / "reviews.csv.gz",
        "collection": "reviews",
        "indexes": [
            [("listing_id", pymongo.ASCENDING)],
            [("id", pymongo.ASCENDING)],
            [("date", pymongo.ASCENDING)],
        ],
    },
}


# ─────────────────────────────────────────────
# 3. Helpers
# ─────────────────────────────────────────────
def human_size(path: Path) -> str:
    """Devuelve tamano legible del archivo."""
    size = path.stat().st_size
    for unit in ["B", "KB", "MB", "GB"]:
        if size < 1024:
            return f"{size:.1f} {unit}"
        size /= 1024
    return f"{size:.1f} TB"


def clean_record(record: dict) -> dict:
    """
    Limpia un registro para ser almacenado en MongoDB:
    - Reemplaza NaN/None con None
    - Convierte numpy types a Python nativos
    """
    cleaned = {}
    for k, v in record.items():
        if pd.isna(v) if not isinstance(v, (list, dict)) else False:
            cleaned[k] = None
        elif hasattr(v, 'item'):   # numpy scalar
            cleaned[k] = v.item()
        else:
            cleaned[k] = v
    return cleaned


def ingest_collection(db, name: str, cfg: dict) -> int:
    """
    Lee el CSV.GZ y lo inserta en MongoDB por lotes.
    Retorna el numero total de documentos insertados.
    """
    col_name = cfg["collection"]
    fpath    = cfg["file"]

    log.info(f"[{name}] ---- Inicio ingesta ----")
    log.info(f"[{name}] Archivo: {fpath.name} ({human_size(fpath)})")

    # Eliminar coleccion previa (idempotente)
    db[col_name].drop()
    log.info(f"[{name}] Coleccion '{col_name}' eliminada (drop)")

    collection  = db[col_name]
    total_ins   = 0
    chunk_n     = 0
    t0          = time.time()

    try:
        reader = pd.read_csv(
            fpath,
            compression="gzip",
            chunksize=CHUNK_SIZE,
            low_memory=False,
            dtype=str,          # leer todo como string -> MongoDB lo almacena textual
                                 # Las transformaciones de tipo se hacen en Fase 3 (ETL)
        )

        for chunk in reader:
            chunk_n += 1
            # Convertir NaN a None para MongoDB
            chunk = chunk.where(pd.notnull(chunk), None)
            records = chunk.to_dict(orient="records")

            if records:
                try:
                    result = collection.insert_many(records, ordered=False)
                    n_inserted = len(result.inserted_ids)
                    total_ins += n_inserted
                    log.info(
                        f"[{name}] Lote {chunk_n:>4}: {n_inserted:>6,} docs "
                        f"| Acumulado: {total_ins:>10,}"
                    )
                except Exception as e:
                    log.error(f"[{name}] Error en lote {chunk_n}: {e}")

        elapsed = time.time() - t0
        rate    = total_ins / elapsed if elapsed > 0 else 0
        log.info(
            f"[{name}] Ingesta completa: {total_ins:,} docs en "
            f"{elapsed:.1f}s ({rate:,.0f} docs/s)"
        )
        return total_ins

    except Exception as exc:
        log.error(f"[{name}] ERROR CRITICO en ingesta: {exc}")
        return 0


def create_indexes(db, col_name: str, index_specs: list) -> None:
    """Crea indices en la coleccion."""
    col = db[col_name]
    for spec in index_specs:
        try:
            idx_name = col.create_index(spec)
            field_str = ", ".join(f[0] for f in spec)
            log.info(f"[{col_name}] Indice creado: {idx_name} ({field_str})")
        except Exception as exc:
            log.warning(f"[{col_name}] Error al crear indice {spec}: {exc}")


def verify_collection(db, col_name: str) -> int:
    """Cuenta documentos en la coleccion y retorna el total."""
    count = db[col_name].count_documents({})
    log.info(f"[{col_name}] Verificacion: {count:,} documentos en MongoDB")
    return count


def sample_document(db, col_name: str) -> None:
    """Muestra un documento de muestra de la coleccion."""
    doc = db[col_name].find_one()
    if doc:
        keys = list(doc.keys())
        log.info(f"[{col_name}] Muestra de campos: {keys[:10]}{'...' if len(keys)>10 else ''}")


# ─────────────────────────────────────────────
# 4. Main
# ─────────────────────────────────────────────
def main():
    log.info("=" * 60)
    log.info(" FASE 1 - INGESTA CSV.GZ -> MONGODB")
    log.info(f" Inicio: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    log.info(f" Base de datos: {MONGO_DB}")
    log.info(f" Chunk size: {CHUNK_SIZE:,} docs/lote")
    log.info(f" Log: {LOG_FILE.name}")
    log.info("=" * 60)

    # -- 4.1 Verificar archivos fuente
    log.info("\n-- Paso 1: Verificando archivos fuente --")
    missing = []
    for name, cfg in COLLECTIONS.items():
        fpath = cfg["file"]
        if fpath.exists():
            log.info(f"  OK  {fpath.name} ({human_size(fpath)})")
        else:
            log.error(f"  FALTA: {fpath}")
            missing.append(str(fpath))

    if missing:
        log.error(f"Archivos faltantes: {missing}. Abortando.")
        sys.exit(1)

    # -- 4.2 Verificar conectividad MongoDB
    log.info("\n-- Paso 2: Verificando conexion a MongoDB --")
    try:
        client = pymongo.MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000)
        ping   = client.admin.command("ping")
        log.info(f"  MongoDB responde: {ping}")
        db = client[MONGO_DB]
        log.info(f"  Base de datos: '{MONGO_DB}'")
    except Exception as exc:
        log.error(f"  No se puede conectar a MongoDB: {exc}")
        sys.exit(1)

    # -- 4.3 Ingesta por coleccion
    log.info("\n-- Paso 3: Ingesta de colecciones --")
    results = {}
    total_start = time.time()

    for name, cfg in COLLECTIONS.items():
        log.info(f"\n{'='*50}")
        log.info(f"Coleccion: {name.upper()}")
        log.info(f"{'='*50}")

        count = ingest_collection(db, name, cfg)
        results[name] = count

    # -- 4.4 Crear indices
    log.info("\n-- Paso 4: Creando indices --")
    for name, cfg in COLLECTIONS.items():
        if results.get(name, 0) > 0:
            create_indexes(db, cfg["collection"], cfg["indexes"])

    # -- 4.5 Verificacion post-carga
    log.info("\n-- Paso 5: Verificacion post-carga --")
    for name, cfg in COLLECTIONS.items():
        verify_collection(db, cfg["collection"])
        sample_document(db, cfg["collection"])

    # -- 4.6 Resumen final
    total_elapsed = time.time() - total_start
    log.info("\n" + "=" * 60)
    log.info(" RESUMEN FASE 1")
    log.info("=" * 60)
    all_ok = True
    for name, count in results.items():
        status = "[OK]" if count > 0 else "[FALLO]"
        log.info(f"  {status} {name:<12}: {count:>12,} documentos")
        if count == 0:
            all_ok = False

    log.info(f"\n  Tiempo total: {total_elapsed:.1f}s")
    log.info(f"  Base de datos MongoDB: {MONGO_DB}")
    log.info(f"  Colecciones cargadas:")
    for col_name in db.list_collection_names():
        log.info(f"    - {col_name}")

    if all_ok:
        log.info("\n  FASE 1 COMPLETADA EXITOSAMENTE")
    else:
        failed = [k for k, v in results.items() if v == 0]
        log.error(f"  FASE 1 CON ERRORES - Colecciones fallidas: {failed}")
        sys.exit(1)

    log.info("=" * 60)


if __name__ == "__main__":
    main()
