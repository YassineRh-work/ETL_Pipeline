import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from dotenv import load_dotenv
import os
import folium
import webbrowser

# Load variables from .env file
load_dotenv()

# Set environment variable for Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

# Replace 'your-actual-api-key' with your OpenWeatherMap API key
api_key = os.getenv("api_weather")

# List of cities to process
cities = ['Paris', 'Saclay', 'Les Ulis']

city_map = folium.Map(location=[46.603354, 1.888334], zoom_start=6)  # Centered on France

for city in cities:
    # Step 1: Extract Data from OpenWeatherMap API
    api_url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

    # Send a request to OpenWeatherMap API
    response = requests.get(api_url)

    # Check if the request is successful
    if response.status_code == 200:
        weather_data = response.json()
        print(f"Data fetched successfully for {city}!")
    else:
        print(f"Error fetching data for {city}: {response.status_code}")
        continue  # Skip to the next city if there's an error

    # Extract relevant data
    data = {
        'city': weather_data['name'],
        'temperature_c': weather_data['main']['temp'] - 273.15,  # Convert Kelvin to Celsius
        'humidity': weather_data['main']['humidity'],
        'timestamp': datetime.utcfromtimestamp(weather_data['dt']),
    }

    # Get the city's latitude and longitude from the API response
    latitude = weather_data['coord']['lat']
    longitude = weather_data['coord']['lon']

    # Format the timestamp for display
    formatted_timestamp = data['timestamp'].strftime('%Y-%m-%d %H:%M:%S')

    # Add a marker with city details
    popup_text = f"""
    City: {data['city']}<br>
    Temperature (°C): {data['temperature_c']:.2f}<br>
    Humidity: {data['humidity']}%<br>
    Timestamp: {formatted_timestamp}
    """
    folium.Marker(
        location=[latitude, longitude],
        popup=popup_text,
        tooltip=data['city']
    ).add_to(city_map)

# Save the map to an HTML file
map_file = "cities_weather_map.html"
city_map.save(map_file)

# Open the map in the default web browser
webbrowser.open(map_file)

print(f"🌍 Map saved as {map_file} with all cities and opened in your browser.")