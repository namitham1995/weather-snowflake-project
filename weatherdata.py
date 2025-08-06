import requests
import json

api_key = "f8c6bd11e9d8cd837b6a6c586024c7f9"
city = "Bangalore"

# API endpoint with metric units (Celsius)
url = f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"

# Make the request
response = requests.get(url)
data = response.json()

# # Check if request was successful
# if response.status_code == 200:
#     print("✅ Weather data fetched successfully!\n")
#     data = response.json()

#     # Pretty print all JSON data
#     print(json.dumps(data, indent=4))

# else:
#     print("❌ Failed to fetch weather data")
#     print("Status Code:", response.status_code)
#     print("Error:", response.json().get("message", "Unknown error"))

print("City:", data["name"])
print("Temperature:", data["main"]["temp"], "°C")
print("Humidity:", data["main"]["humidity"], "%")
print("Weather:", data["weather"][0]["description"])
