from fastapi import FastAPI
import psycopg2
from psycopg2.extras import RealDictCursor

app = FastAPI()

def get_db_connection():
    return psycopg2.connect(
        dbname="airflow",
        user="airflow",
        password="airflow",
        host="localhost",
        port="5432",
        cursor_factory=RealDictCursor
    )

@app.get("/gold-prices/latest")
async def get_latest_gold_price():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM gold_prices ORDER BY id DESC;")
        result = cursor.fetchall()
        conn.close()
        if result:
            return {"status": "success", "data": result}
        else:
            return {"status": "error", "message": "No data found"}
    except Exception as e:
        return {"status": "error", "message": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
