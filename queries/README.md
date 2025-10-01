# Queries Container – Proyecto BDNR

Este contenedor ejecuta **5 consultas** sobre la base `yfinance_data` en MongoDB Atlas.  
Las consultas incluyen:

1. Precio promedio de AAPL en Q1 2023.  
2. Precio máximo alcanzado por cada acción en H1 2024.  
3. Top 5 acciones con mayores dividendos en 2022.  
4. Acción con menor valor el 16 de agosto de 2024.  
5. Volumen total de las primeras 10 acciones (A, AAPL, ABBV, ABNB, ABT, ACGL, ACN, ADBE, ADI, ADM).  

---

## Cómo construir la imagen

Dentro de esta carpeta (`queries/`):

```bash
docker build -t queries .


## Como correr el contenedor:

docker run --rm queries

