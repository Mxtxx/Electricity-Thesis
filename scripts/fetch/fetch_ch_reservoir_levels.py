import pandas as pd
import requests
from pathlib import Path

#Direct link to the CSV data from opendata.swiss
URL = "https://www.uvek-gis.admin.ch/BFE/ogd/17/ogd17_fuellungsgrad_speicherseen.csv"

#Output path
outdir = Path("data/raw")
outdir.mkdir(parents=True, exist_ok=True)
outfile = outdir / "ch_reservoir_levels_weekly.csv"

print("Fetching weekly Swiss reservoir levels")
r = requests.get(URL)
r.raise_for_status()  #error if failed

with open(outfile, "wb") as f:
    f.write(r.content)

print(f"Saved raw reservoir levels to {outfile}")
