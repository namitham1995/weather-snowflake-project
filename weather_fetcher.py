# import os
import time
import json
import boto3
import requests
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("WeatherData")

API_KEY = "f8c6bd11e9d8cd837b6a6c586024c7f9"
CITIES = ["Bangalore", "Delhi", "Mumbai"]  # ✅ Add more cities here

def lambda_handler(event, context):
    for city in CITIES:
        try:
            url = (
                f"https://api.openweathermap.org/data/2.5/weather?"
                f"q={city}&appid={API_KEY}&units=metric"
            )
            resp = requests.get(url, timeout=10)
            data = resp.json()

            print(f"Fetched weather data for {city}: {data['weather'][0]['main']}")

            item = {
                "city": city,
                "timestamp": int(time.time()),
                "temperature": Decimal(str(data["main"]["temp"])),
                "feels_like": Decimal(str(data["main"]["feels_like"])),
                "humidity": Decimal(str(data["main"]["humidity"])),
                "pressure": Decimal(str(data["main"]["pressure"])),
                "wind_speed": Decimal(str(data["wind"].get("speed", 0))),
                "wind_gust": Decimal(str(data["wind"].get("gust", 0))),
                "cloud_coverage": Decimal(str(data["clouds"].get("all", 0))),
                "weather_main": data["weather"][0]["main"],
                "weather_desc": data["weather"][0]["description"],
                "sunrise": int(data["sys"]["sunrise"]),
                "sunset": int(data["sys"]["sunset"]),
                "raw": json.dumps(data)
            }

            table.put_item(Item=item)
            print(f"✅ Stored weather data for {city} in DynamoDB.")
        
        except Exception as e:
            print(f"❌ Error fetching data for {city}: {e}")

    return {
        "statusCode": 200,
        "body": f"Stored weather for {len(CITIES)} cities"
    }
