import requests
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv



load_dotenv()
API_KEY = os.getenv("OPENWEATHER_API_KEY")


CITY = "Cebu"
DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "database": "weather_data",
    "user": "admin",
    "password": "password123"
}

def fetch_weather():
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    return None

def save_to_db(data):
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        
        # Create a "Bronze" (Raw) table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS raw_weather (
                id SERIAL PRIMARY KEY,
                city VARCHAR(50),
                temperature FLOAT,
                humidity INT,
                weather_desc VARCHAR(100),
                recorded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Insert the data
        insert_query = """
            INSERT INTO raw_weather (city, temperature, humidity, weather_desc)
            VALUES (%s, %s, %s, %s)
        """
        cur.execute(insert_query, (
            data['name'], 
            data['main']['temp'], 
            data['main']['humidity'], 
            data['weather'][0]['description']
        ))
        
        conn.commit()
        print(f"✅ Data for {CITY} saved to database!")
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Database Error: {e}")

if __name__ == "__main__":
    weather_json = fetch_weather()
    if weather_json:
        save_to_db(weather_json)
    else:
        print("❌ Failed to fetch data. Check your API Key!")