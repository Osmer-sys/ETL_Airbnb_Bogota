"""Script de consulta rapida a MongoDB para visualizar los datos cargados."""
import pymongo
import json
from datetime import datetime

client = pymongo.MongoClient("mongodb://localhost:27017/")
db = client["airbnb_bogota"]

SEP  = "=" * 65
SEP2 = "-" * 65

print(SEP)
print(" MONGODB :: airbnb_bogota")
print(f" Consultado: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print(SEP)

# ── ESTADISTICAS GENERALES ──────────────────────────────────────
collections_info = {}
for col_name in ["listings", "calendar", "reviews"]:
    col = db[col_name]
    count = col.count_documents({})
    collections_info[col_name] = count

print("\n RESUMEN GENERAL")
print(SEP2)
print(f"  {'Coleccion':<15} {'Documentos':>15}  {'Campos':>8}")
print(SEP2)
for col_name, count in collections_info.items():
    n_campos = len(db[col_name].find_one()) - 1  # -1 para _id
    print(f"  {col_name:<15} {count:>15,}  {n_campos:>8}")
print(SEP2)
total = sum(collections_info.values())
print(f"  {'TOTAL':<15} {total:>15,}")

# ── DETALLE POR COLECCION ───────────────────────────────────────
for col_name in ["listings", "calendar", "reviews"]:
    col = db[col_name]
    count = col.count_documents({})

    print(f"\n\n{'#'*65}")
    print(f"  COLECCION: {col_name.upper()}  ({count:,} documentos)")
    print(f"{'#'*65}")

    # Campos
    sample_doc = col.find_one()
    campos = [k for k in sample_doc.keys() if k != "_id"]
    print(f"\n  Campos ({len(campos)} total):")
    # Mostrar en columnas de 4
    for i in range(0, len(campos), 4):
        row = campos[i:i+4]
        print("    " + "  |  ".join(f"{c:<30}" for c in row))

    # 5 documentos de muestra
    print(f"\n  Muestra de documentos (primeros 3):")
    print(SEP2)
    for i, doc in enumerate(col.find({}, {"_id": 0}).limit(3), 1):
        print(f"\n  Documento #{i}:")
        for k, v in doc.items():
            val_str = str(v) if v is not None else "NULL"
            if len(val_str) > 70:
                val_str = val_str[:70] + "..."
            print(f"    {k:<40} : {val_str}")
        print()

    # Stats especificas por coleccion
    if col_name == "listings":
        print("  Distribucion por room_type:")
        pipeline = [{"$group": {"_id": "$room_type", "total": {"$sum": 1}}},
                    {"$sort": {"total": -1}}]
        for r in col.aggregate(pipeline):
            print(f"    {str(r['_id']):<35} : {r['total']:>6,}")

        print("\n  Top 5 neighbourhoods:")
        pipeline2 = [{"$group": {"_id": "$neighbourhood_cleansed", "total": {"$sum": 1}}},
                     {"$sort": {"total": -1}}, {"$limit": 5}]
        for r in col.aggregate(pipeline2):
            print(f"    {str(r['_id']):<35} : {r['total']:>6,}")

    elif col_name == "calendar":
        print("  Distribucion available (t=true, f=false):")
        pipeline = [{"$group": {"_id": "$available", "total": {"$sum": 1}}},
                    {"$sort": {"total": -1}}]
        for r in col.aggregate(pipeline):
            print(f"    {str(r['_id']):<10} : {r['total']:>10,}")

        print("\n  Rango de fechas:")
        minmax = list(col.aggregate([{"$group": {
            "_id": None,
            "fecha_min": {"$min": "$date"},
            "fecha_max": {"$max": "$date"}
        }}]))
        if minmax:
            print(f"    Desde: {minmax[0]['fecha_min']}")
            print(f"    Hasta: {minmax[0]['fecha_max']}")

    elif col_name == "reviews":
        print("  Rango de fechas de reviews:")
        minmax = list(col.aggregate([{"$group": {
            "_id": None,
            "fecha_min": {"$min": "$date"},
            "fecha_max": {"$max": "$date"}
        }}]))
        if minmax:
            print(f"    Primera review: {minmax[0]['fecha_min']}")
            print(f"    Ultima review : {minmax[0]['fecha_max']}")

        print("\n  Top 5 listings con mas reviews:")
        pipeline = [{"$group": {"_id": "$listing_id", "total": {"$sum": 1}}},
                    {"$sort": {"total": -1}}, {"$limit": 5}]
        for r in col.aggregate(pipeline):
            print(f"    listing_id {str(r['_id']):<15} : {r['total']:>6,} reviews")

print(f"\n{SEP}")
print(" CONEXION CERRADA - MongoDB airbnb_bogota OK")
print(SEP)
client.close()
