from pymongo import MongoClient
from datetime import datetime

# Conexión a Mongo Atlas con usuario/contraseña
uri = "mongodb+srv://lafarga_db_user:GMKmxXic3m1apvn2@cluster0.gwoisn4.mongodb.net/"
client = MongoClient(uri)

# Selección de la base y la colección
db = client["yfinance_data"]
col = db["acciones_historicas_sp500"]

# 1) Precio promedio de AAPL en Q1 2023
print("\n1) Precio promedio de AAPL en Q1 2023")
q1 = col.aggregate([
    {"$match": {
        "ticker": "AAPL",
        "fecha": {"$gte": datetime(2023, 1, 1), "$lte": datetime(2023, 3, 31)}
    }},
    {"$group": {"_id": "$ticker", "precioPromedio": {"$avg": "$cierre"}}}
])
for r in q1:
    print(r)

# 2) Precio máximo por acción en H1 2024
print("\n2) Precio máximo por acción en H1 2024")
q2 = col.aggregate([
    {"$match": {"fecha": {"$gte": datetime(2024, 1, 1), "$lte": datetime(2024, 6, 30)}}},
    {"$group": {"_id": "$ticker", "maxPrecio": {"$max": "$maximo"}}},
    {"$sort": {"maxPrecio": -1}}
])
for r in q2:
    print(r)

# 3) Top 5 acciones por dividendos en 2022
print("\n3) Top 5 acciones por dividendos en 2022")
q3 = col.aggregate([
    {"$match": {
        "fecha": {"$gte": datetime(2022, 1, 1), "$lte": datetime(2022, 12, 31)},
        "dividendos": {"$gt": 0}
    }},
    {"$group": {"_id": "$ticker", "totalDividendos": {"$sum": "$dividendos"}}},
    {"$sort": {"totalDividendos": -1}},
    {"$limit": 5}
])
for r in q3:
    print(r)

# 4) Acción de menor valor el 16 agosto 2024
print("\n4) Acción de menor valor el 16 agosto 2024")
q4 = col.aggregate([
    {"$match": {"fecha": datetime(2024, 8, 16, 4, 0, 0)}},
    {"$project": {"ticker": 1, "cierre": 1}},
    {"$sort": {"cierre": 1}},
    {"$limit": 1}
])
for r in q4:
    print(r)

# 5) Volumen total de primeras 10 acciones (alfabéticamente)
print("\n5) Volumen total de primeras 10 acciones (A, AAPL, ABBV, ABNB, ABT, ACGL, ACN, ADBE, ADI, ADM)")
q5 = col.aggregate([
    {"$match": {"ticker": {"$in": ["A", "AAPL", "ABBV", "ABNB", "ABT", "ACGL", "ACN", "ADBE", "ADI", "ADM"]}}},
    {"$group": {"_id": "$ticker", "volumenTotal": {"$sum": "$volumen"}}},
    {"$project": {"_id": 1, "volumenTotal": {"$toDouble": "$volumenTotal"}}},
    {"$sort": {"_id": 1}}
])
for r in q5:
    print(r)

