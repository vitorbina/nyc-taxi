# NYC Weather Ingestion

This pipeline fetches daily historical weather data for New York City and stores it in the data lake. The data comes from [Open-Meteo](https://open-meteo.com/), a free archive API that requires no authentication and covers historical records going back to 1940.

Each run fetches a full day of hourly readings for NYC (40.7128°N, 74.0060°W) in the `America/New_York` timezone and saves the result as a JSON file:

```
raw/weather/partition_date=2024-01-15/weather_nyc_2024-01-15.json
```

The pipeline runs daily (`@daily`) starting from January 2024, with 2 retries on failure.

## What gets collected

Each file contains 24 hourly records covering:

- `temperature_2m` — air temperature at 2m (°C)
- `apparent_temperature` — feels-like temperature (°C)
- `relative_humidity_2m` — relative humidity (%)
- `precipitation` — total precipitation (mm)
- `wind_speed_10m` — wind speed at 10m (km/h)
- `wind_direction_10m` — wind direction (°)
- `weather_code` — WMO weather condition code

## Data availability

Open-Meteo's archive has a lag of approximately 5 days (d-5). Since this pipeline is designed to work alongside monthly taxi data, the delay has no practical impact on the workflow.

## References

- [Open-Meteo Archive API](https://open-meteo.com/en/docs/historical-weather-api)
- [WMO Weather Interpretation Codes](https://open-meteo.com/en/docs/historical-weather-api#weathervariables)
