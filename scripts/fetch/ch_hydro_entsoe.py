from entsoe import EntsoePandasClient
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

#setup
load_dotenv()
api_key = os.getenv("ENTSOE_API_KEY")
client = EntsoePandasClient(api_key=api_key)

start = pd.Timestamp("2021-03-22", tz="Europe/Zurich")
end = pd.Timestamp("2025-01-01", tz="Europe/Zurich")

#output folder
outdir= Path("data/raw")
outdir.mkdir(parents=True, exist_ok=True)

#getting actual hydro data from entsoe
#PSR codes see entsoe
# B15 = Hydro Run-of-river and poundage
# B14 = Hydro Water Reservoir
# B06 = Hydro Pumped Storage
# psr codes mapping to names
psr_codes={
    "B15": "hydro_ror_mw",
    "B14": "hydro_reservoir_mw",
    "B06": "hydro_pumped_mw"
}
    
all_df=[]

for psr, colname in psr_codes.items():
    print("Fetching Swiss generation for {colname} ({psr})")
    try:
        g = client.query_generation(country_code="CH", start=start, end=end, psr_type=psr)
        g = g.rename(columns={g.columns[0]: colname}) if hasattr(g, "columns") else g.to_frame(colname)
        all_df.append(g)
    except Exception as e:
        print("Failed for {psr}: {e}")
    
if all_df:
    mix = pd.concat(all_df, axis=1).sort_index()
    out= outdir / "ch_hydro_generation_entsoe.csv"
    mix.to_csv(out)
    print ("Succeeded")

else:
    print("No entsoe data retrieved")


