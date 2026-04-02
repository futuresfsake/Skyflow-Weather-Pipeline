
# used bronze raw data, bronze layer that contains the rawest version of your data
#Medallion architecture also known as multi-hop architecture,. this design pattern is used to organzie data within a lakehouse, like databricks or snowflake

import psycopg2

DB_CONFIG = {
    "host": "localhost",
    "port": "5433", # Make sure this matches your new port!
    "database": "weather_data",
    "user": "admin",
    "password": "password123"
}

def peek_at_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        cur.execute("SELECT * FROM clean weather ORDER BY recorded_at DESC LIMIT 5;")
        rows = cur.fetchall()
        
        print("\n--- 🌦️ LATEST WEATHER DATA IN DATABASE ---")
        for row in rows:
            print(f"City: {row[1]} | Temp: {row[2]}°C | Desc: {row[4]} | Time: {row[5]}")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    peek_at_data()