from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook # This is used for PostgreSQL
from airflow.decorators import task # This is used for task decorators
from airflow.utils import timezone
from datetime import timedelta # This is used for setting the start date
import requests
import json
import os
from dotenv import load_dotenv
load_dotenv()

start_date = timezone.utcnow() - timedelta(days=3)
LATITUDE = '51.2362'
LONGITUDE = '-0.5704'

POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_weather_api'   

# Load environment variables from .env file
OPENWEATHER_API_KEY = os.getenv('OPEN_WEATHER_API_KEY')

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="etlweather",
    default_args=default_args,
    description="A simple ETL DAG for weather data",
    schedule="@daily",
    start_date=timezone.utcnow() - timedelta(days=3),
    catchup=False,
) as dag:

    @task
    def extract():
        http = HttpHook(method='GET', http_conn_id=API_CONN_ID)
        url = f'data/3.0/onecall?lat={LATITUDE}&lon={LONGITUDE}&exclude=minutely,alerts&appid={OPENWEATHER_API_KEY}'
        response = http.run(url)
        if response.status_code == 200:
            data = response.json()
            return data
        else:
            raise Exception(f"API request failed with status code {response.status_code}")

    @task
    def transform(data):
        def safe_weather_description(obj):
            weather = obj.get("weather", [])
            if weather and isinstance(weather, list) and isinstance(weather[0], dict):
                return weather[0].get("description", "")
            return ""

        transformed_data = {
            'location': {
                'latitude': data.get('lat'),
                'longitude': data.get('lon'),
                'timezone': data.get('timezone'),
                'timezone_offset': data.get('timezone_offset')
            },
            'current': {
                'temp': data.get('current', {}).get('temp'),
                'feels_like': data.get('current', {}).get('feels_like'),
                'pressure': data.get('current', {}).get('pressure'),
                'humidity': data.get('current', {}).get('humidity'),
                'uvi': data.get('current', {}).get('uvi'),
                'clouds': data.get('current', {}).get('clouds'),
                'visibility': data.get('current', {}).get('visibility'),
                'wind_speed': data.get('current', {}).get('wind_speed'),
                'wind_deg': data.get('current', {}).get('wind_deg'),
                'weather': safe_weather_description(data.get('current', {}))
            },
            'hourly': [
                {
                    'time': hour.get('dt'),
                    'temp': hour.get('temp'),
                    'feels_like': hour.get('feels_like'),
                    'pressure': hour.get('pressure'),
                    'humidity': hour.get('humidity'),
                    'weather': safe_weather_description(hour),
                    'pop': hour.get('pop'),
                    'wind_speed': hour.get('wind_speed'),
                    'wind_deg': hour.get('wind_deg')
                }
                for hour in data.get('hourly', [])
                if hour.get('dt') is not None  
            ],
            'daily': [
                {
                    'date': day.get('dt'),
                    'sunrise': day.get('sunrise'),
                    'sunset': day.get('sunset'),
                    'temp_max': day.get('temp', {}).get('max'),
                    'temp_min': day.get('temp', {}).get('min'),
                    'humidity': day.get('humidity'),
                    'pressure': day.get('pressure'),
                    'wind_speed': day.get('wind_speed'),
                    'wind_deg': day.get('wind_deg'),
                    'weather': safe_weather_description(day),
                    'rain': day.get('rain')
                }
                for day in data.get('daily', [])
                if day.get('dt') is not None and day.get('temp', {})  
            ]
        }
        return transformed_data

    @task
    def load(data):
        """Load weather data into PostgreSQL, handling input as dict or string."""

        if isinstance(data, str):
            data = json.loads(data)

        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_location (
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                timezone VARCHAR(255),
                timezone_offset INT,
                PRIMARY KEY (latitude, longitude)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_current (
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                temp FLOAT,
                feels_like FLOAT,
                pressure INT,
                humidity INT,
                uvi FLOAT,
                clouds INT,
                visibility INT,
                wind_speed FLOAT,
                wind_deg FLOAT,
                weather VARCHAR(255),
                PRIMARY KEY (latitude, longitude)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_hourly (
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                time TIMESTAMP,
                temp FLOAT,
                feels_like FLOAT,
                pressure INT,
                humidity INT,
                pop FLOAT,
                wind_speed FLOAT,
                wind_deg FLOAT,
                weather VARCHAR(255),
                PRIMARY KEY (latitude, longitude, time)
            );
        """)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_daily (
                latitude FLOAT NOT NULL,
                longitude FLOAT NOT NULL,
                date TIMESTAMP,
                temp_max FLOAT,
                temp_min FLOAT,
                humidity INT,
                pressure INT,
                wind_speed FLOAT,
                wind_deg FLOAT,
                weather VARCHAR(255),
                rain FLOAT,
                PRIMARY KEY (latitude, longitude, date)
            );
        """)

        loc = data.get('location', {})
        cursor.execute("""
            INSERT INTO weather_location (latitude, longitude, timezone, timezone_offset)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (latitude, longitude) DO NOTHING;
        """, (
            loc.get('latitude'),
            loc.get('longitude'),
            loc.get('timezone'),
            loc.get('timezone_offset'),
        ))

        curr = data.get('current', {})
        cursor.execute("""
            INSERT INTO weather_current (
                latitude, longitude, temp, feels_like, pressure, humidity, uvi, clouds,
                visibility, wind_speed, wind_deg, weather
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (latitude, longitude) DO NOTHING;
        """, (
            loc.get('latitude'),
            loc.get('longitude'),
            curr.get('temp'),
            curr.get('feels_like'),
            curr.get('pressure'),
            curr.get('humidity'),
            curr.get('uvi'),
            curr.get('clouds'),
            curr.get('visibility'),
            curr.get('wind_speed'),
            curr.get('wind_deg'),
            curr.get('weather')
        ))

        for hour in data.get('hourly', []):
            if hour.get('time') is None:
                continue
            cursor.execute("""
                INSERT INTO weather_hourly (
                    latitude, longitude, time, temp, feels_like, pressure, humidity,
                    pop, wind_speed, wind_deg, weather
                )
                VALUES (%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (latitude, longitude, time) DO NOTHING;
            """, (
                loc.get('latitude'),
                loc.get('longitude'),
                hour.get('time'),
                hour.get('temp'),
                hour.get('feels_like'),
                hour.get('pressure'),
                hour.get('humidity'),
                hour.get('pop'),
                hour.get('wind_speed'),
                hour.get('wind_deg'),
                hour.get('weather')
            ))

        for day in data.get('daily', []):
            if day.get('date') is None:
                continue
            cursor.execute("""
                INSERT INTO weather_daily (
                    latitude, longitude, date, temp_max, temp_min, humidity, pressure,
                    wind_speed, wind_deg, weather, rain
                )
                VALUES (%s, %s, to_timestamp(%s), %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (latitude, longitude, date) DO NOTHING;
            """, (
                loc.get('latitude'),
                loc.get('longitude'),
                day.get('date'),
                day.get('temp_max'),
                day.get('temp_min'),
                day.get('humidity'),
                day.get('pressure'),
                day.get('wind_speed'),
                day.get('wind_deg'),
                day.get('weather'),
                day.get('rain', 0),
            ))

        conn.commit()
        cursor.close()
        conn.close()


    weather_data = extract()
    transformed_data = transform(weather_data)
    load(transformed_data)