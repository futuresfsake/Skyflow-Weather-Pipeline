import requests
import psycopg2
import os
from datetime import datetime
from dotenv import load_dotenv

# 1. Load the environment variables from .env file
load_dotenv()

# 2. Get Credentials from Environment
API_KEY = os.getenv("OPENWEATHER_API_KEY")
CITY = "Cebu"

# It's better to fetch DB credentials from .env too!
DB_CONFIG = {
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
    "database": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASS")
}

def fetch_weather():
    if not API_KEY:
        print("❌ Error: OPENWEATHER_API_KEY not found in .env")
        return None
        
    url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
    
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        elif response.status_code == 401:
            print("❌ API Error: Unauthorized. Check if your API Key is active.")
        else:
            print(f"❌ API Error {response.status_code}: {response.text}")
    except Exception as e:
        print(f"❌ Connection Error: {e}")
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