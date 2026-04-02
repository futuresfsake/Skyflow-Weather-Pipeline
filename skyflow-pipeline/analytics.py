#this script will perform an Aggregation


import psycopg2
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "host": "localhost",
    "port": "5433",
    "database": "weather_data",
    "user": "admin",
    "password": "password123"
}

def create_gold_view():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()

        # Create a View (a virtual table) for daily summaries
        # This is a classic "Gold" layer move
        gold_query = """
            CREATE OR REPLACE VIEW daily_weather_summary AS
            SELECT 
                city,
                DATE(recorded_at) as report_date,
                ROUND(AVG(temp_celsius)::numeric, 2) as avg_temp,
                MAX(temp_celsius) as max_temp,
                COUNT(*) as readings_count
            FROM clean_weather
            GROUP BY city, report_date
            ORDER BY report_date DESC;
        """
        
        cur.execute(gold_query)
        conn.commit()
        
        print("🏆 Gold View Created: You now have a summary table for analytics!")
        
        cur.close()
        conn.close()
    except Exception as e:
        print(f"❌ Analytics Error: {e}")

if __name__ == "__main__":
    create_gold_view()