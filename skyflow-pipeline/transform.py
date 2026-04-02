import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "database": "weather_data",
    "user": "admin",
    "password": "password123"
}

def transform_and_load():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # 1. Create the Silver Table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS clean_weather (
                city VARCHAR(50),
                temp_celsius FLOAT,
                humidity INT,
                condition VARCHAR(100),
                recorded_at TIMESTAMP,
                PRIMARY KEY (city, recorded_at)
            );
        """)

        # 2. Extract from Bronze and Insert into Silver
        # We use 'DISTINCT' to avoid duplicates
        transform_query = """
            INSERT INTO clean_weather (city, temp_celsius, humidity, condition, recorded_at)
            SELECT DISTINCT 
                city, 
                ROUND(temperature::numeric, 1), 
                humidity, 
                UPPER(weather_desc), 
                recorded_at
            FROM raw_weather
            ON CONFLICT (city, recorded_at) DO NOTHING;
        """
        
        cur.execute(transform_query)
        conn.commit()
        
        print("✨ Silver Transformation Complete: Data cleaned and deduplicated!")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Transformation Error: {e}")

if __name__ == "__main__":
    transform_and_load()