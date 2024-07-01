import datetime
import json
import requests
import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
from modal import App, web_endpoint

app = App("starfish")

# Set up the Open-Meteo API client with cache and retry on error
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)


@app.function()
@web_endpoint(method="GET")
def get_coords(postcode: str):
    # Return latitude and longitude for a UK postcode
    request_body = f"https://api.postcodes.io/postcodes/{postcode}"
    location = requests.get(request_body)
    longitude = location.json()['result']['longitude']
    latitude = location.json()['result']['latitude']
    return {"latitude": latitude, "longitude": longitude}


def process_weather(responses):
    response = responses[0]
    # Process hourly data. The order of variables needs to be the same as requested.
    hourly = response.Hourly()
    hourly_temperature_2m = hourly.Variables(0).ValuesAsNumpy()
    hourly_cloud_cover = hourly.Variables(1).ValuesAsNumpy()
    hourly_rain = hourly.Variables(2).ValuesAsNumpy()

    hourly_data = {"date": pd.date_range(
        start=pd.to_datetime(hourly.Time(), unit="s", utc=True),
        end=pd.to_datetime(hourly.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=hourly.Interval()),
        inclusive="left"
    ), "temperature_2m": hourly_temperature_2m, "cloud_cover": hourly_cloud_cover, "rain": hourly_rain}
    hourly_dataframe = pd.DataFrame(data=hourly_data).set_index('date')
    result = hourly_dataframe.to_json(orient="index")
    parsed = json.loads(result)
    return parsed


@app.function()
@web_endpoint(method="GET")
def get_weather(latitude: float, longitude: float, days: int):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": ["temperature_2m", "cloud_cover", "rain"],
        "forecast_days": days
    }
    responses = openmeteo.weather_api(url, params=params)
    parsed = process_weather(responses)
    return parsed
