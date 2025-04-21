# 🌤️ Weather ETL and Visualization Project

## Overview

This project fetches real-time weather data from the [OpenWeatherMap API](https://openweathermap.org/api) and performs two main operations:

1. `main.py`: Extracts weather data for **New York**, transforms it, and loads it into **Google BigQuery**.
2. `weather_map.py`: Extracts weather data for a list of cities and displays it on an interactive **Folium** map.

---

## 🔧 Setup

```bash
### 1. Install Dependencies

pip install requests pandas google-cloud-bigquery python-dotenv folium


### 2. Environment Configuration

Create a .env file in the project root with the following content:
api_weather=your_openweathermap_api_key
GOOGLE_APPLICATION_CREDENTIALS=path_to_your_google_credentials.json

📜 Script Descriptions
main.py – ETL to BigQuery
Extract: Weather data for New York using OpenWeatherMap API.
Transform: Convert temperature from Kelvin to Fahrenheit and clean data using Pandas.
Load: Push the final DataFrame into Google BigQuery.
Output:
DataFrame is printed to console.
Data is loaded to the specified BigQuery table with a success message.

weather_map.py – Weather Visualization Map
Targets cities: Paris, Saclay, Les Ulis.
Extract: Weather data from OpenWeatherMap API.
Visualize: Creates a map using Folium with markers for each city showing:
Temperature (°C)
Humidity
Timestamp
Output:
HTML file cities_weather_map.html with an interactive map.
Automatically opens the map in your default browser.



