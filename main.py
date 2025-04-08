import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
from dotenv import load_dotenv
import os

# Load variables from .env file
load_dotenv()

# Set environment variable for Google credentials
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
# Step 1: Extract Data from OpenWeatherMap API

# Replace 'your-actual-api-key' with your OpenWeatherMap API key
api_key = os.getenv("api_weather")
city = 'New York'
api_url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}'

# Send a request to OpenWeatherMap API
response = requests.get(api_url)

# Check if the request is successful
if response.status_code == 200:
    weather_data = response.json()
    print("Data fetched successfully!")
else:
    print(f"Error fetching data: {response.status_code}")
    exit()

# Step 2: Transform the Data using pandas
# Extract relevant data
data = {
    'city': weather_data['name'],
    'temperature_c': weather_data['main']['temp'] - 273.15,  # Convert Kelvin to Celsius
    'humidity': weather_data['main']['humidity'],
    'timestamp': datetime.utcfromtimestamp(weather_data['dt']),
}

# Create a DataFrame
df = pd.DataFrame([data])

# Transform: Convert temperature to Fahrenheit
df['temperature_f'] = df['temperature_c'] * 9/5 + 32

# Drop the Celsius column
df = df.drop(columns=['temperature_c'])

# Print the transformed DataFrame
print(df)

# Step 3: Load the Transformed Data into BigQuery

# Set up BigQuery client
client = bigquery.Client()

# Specify the dataset and table name
dataset_id = 'etlpipeline-456207.weather_dataset'  # Replace with your Google Cloud project ID and dataset name
table_id = f"{dataset_id}.weather_data"  # Table name where data will be loaded

# Load data to BigQuery
job = client.load_table_from_dataframe(df, table_id)

# Wait for the job to complete
job.result()

# Confirmation message
print(f"✅ Data uploaded to {table_id} successfully!")
