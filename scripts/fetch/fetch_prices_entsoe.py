from entsoe import EntsoePandasClient
import pandas as pd
import os
from dotenv import load_dotenv

#getting the api key
load_dotenv()
api_key = os.getenv("ENTSOE_API_KEY")

#creating the client & defining time range
client = EntsoePandasClient(api_key=api_key)

start = pd.Timestamp("2020-01-01", tz="Europe/Zurich")
end = pd.Timestamp("2025-01-01", tz="Europe/Zurich")

#creating loop for the different bidding zones
bidding_zones = {
    "CH": "Switzerland",
    "DE_LU": "Germany",
    "FR": "France",
    "IT_NORD": "Italy North"
}

for zone_code, country in bidding_zones.items():
    print(f"Fetching day-ahead prices for {country} ({zone_code})...")
    
    df = client.query_day_ahead_prices(country_code=zone_code,start=start,end=end)

    #save to CSV
    output_path = f"data/raw/{zone_code.lower()}_day_ahead_prices.csv"
    df.to_csv(output_path)
    print(f"Saved {country} prices to {output_path}")

