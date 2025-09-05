import requests
import os
from dotenv import load_dotenv

print("Starting API test...")

# Load environment variables
load_dotenv()
print("Environment loaded")

# Test Alpha Vantage
av_key = os.getenv('ALPHA_VANTAGE_API_KEY')
print(f"Alpha Vantage key loaded: {av_key is not None}")

if av_key:
    response = requests.get(f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol=WMT&apikey={av_key}")
    print("Alpha Vantage status:", response.status_code)
else:
    print("No Alpha Vantage key found!")

# Test OpenWeatherMap  
ow_key = os.getenv('OPENWEATHER_API_KEY')
print(f"OpenWeatherMap key loaded: {ow_key is not None}")

if ow_key:
    response = requests.get(f"http://api.openweathermap.org/data/2.5/weather?q=Bentonville,AR,US&appid={ow_key}")
    print("OpenWeatherMap status:", response.status_code)
else:
    print("No OpenWeatherMap key found!")

print("Test complete!")