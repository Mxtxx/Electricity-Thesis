from entsoe import EntsoePandasClient
import pandas as pd
import os
from dotenv import load_dotenv

#getting the api key
load_dotenv()
api_key = os.getenv("ENTSOE_API_KEY")

#creating the client & defining time range
client = EntsoePandasClient(api_key=api_key)

start = pd.Timestamp("2021-03-22", tz="Europe/Zurich")
end = pd.Timestamp("2025-01-01", tz="Europe/Zurich")

#creating loop for the different bidding zones
bidding_zones = {
    "CH": "Switzerland",
    "DE_LU": "Germany",
    "FR": "France",
    "IT_NORD": "Italy North"
}

for zone_code, country in bidding_zones.items():
    print(f"Fetching day-ahead load forecast for {country} ({zone_code})")

    try:
        #query ENTSO-E Day-Ahead Load Forecast (processType=A01)
        df = client.query_load_forecast(
            country_code=zone_code,
            start=start,
            end=end,
            process_type="A01"
        )

        #convert to DataFrame if Series
        if isinstance(df, pd.Series):
            df = df.to_frame(name="load_forecast_mw")

        #save to CSV
        output_path = f"data/raw/{zone_code.lower()}_load_forecast.csv"
        df.to_csv(output_path)
        print(f"Saved {country} load forecast to {output_path}")

    except Exception as e:
        print(f"Failed to fetch {country} ({zone_code}): {e}")