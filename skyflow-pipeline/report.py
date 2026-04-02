import psycopg2

def show_report():
    conn = psycopg2.connect(host="localhost", port="5433", database="weather_data", user="admin", password="password123")
    cur = conn.cursor()
    
    cur.execute("SELECT * FROM daily_weather_summary;")
    rows = cur.fetchall()
    
    print("\n--- 📊 EXECUTIVE WEATHER SUMMARY (GOLD LAYER) ---")
    print(f"{'City':<10} | {'Date':<12} | {'Avg Temp':<8} | {'Max Temp':<8} | {'Readings'}")
    print("-" * 60)
    for row in rows:
        print(f"{row[0]:<10} | {str(row[1]):<12} | {row[2]:<8}°C | {row[3]:<8}°C | {row[4]}")
    
    cur.close()
    conn.close()

if __name__ == "__main__":
    show_report()