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
#cross-border connections (always CH on one side)
crossborder_links = {
    ("CH", "DE_LU"): "Switzerland ↔ Germany-Luxembourg",
    ("CH", "FR"): "Switzerland ↔ France",
    ("CH", "IT_NORD"): "Switzerland ↔ Italy North"
}

#fetch loop
for country_from, country_to in crossborder_links.keys():
    description = crossborder_links[(country_from, country_to)]
    print(f"Fetching NTC for {description} ({country_from} <-> {country_to})...")

    try:
        #query ENTSO-E Transmission Capacities (Document A09)
        df = client.query_net_transfer_capacity_dayahead(
            country_code_from=country_from,
            country_code_to=country_to,
            start=start,
            end=end
        )

        #convert to DataFrame if Series
        if isinstance(df, pd.Series):
            df = df.to_frame(name="ntc_mw")

        #save to CSV
        output_path = f"data/raw/ntc_{country_from.lower()}_{country_to.lower()}.csv"
        df.to_csv(output_path)
        print(f"Saved NTC {description} to {output_path}")

    except Exception as e:
        print(f"Failed to fetch NTC {description}: {e}")