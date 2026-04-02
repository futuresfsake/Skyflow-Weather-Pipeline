import time
from ingest import fetch_weather, save_to_db
from transform import transform_and_load
from analytics import create_gold_view

def run_pipeline():
    print(f"🚀 [{time.strftime('%Y-%m-%d %H:%M:%S')}] Starting SkyFlow Pipeline...")
    
    weather_data = fetch_weather()
    if weather_data:
        save_to_db(weather_data)
        transform_and_load()
        create_gold_view()
        print("✅ Pipeline finished successfully!")
    else:
        print("⚠️ Pipeline stopped: Could not fetch new data.")

if __name__ == "__main__":
    print("⏲️ Automation Active. Press Ctrl+C to stop.")
    while True:
        run_pipeline()
        print("😴 Sleeping for 1 hour... Zzz...")
        time.sleep(3600)  # 3600 seconds = 1 hour