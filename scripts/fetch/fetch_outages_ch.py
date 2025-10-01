from entsoe import EntsoePandasClient
import pandas as pd
import os
from dotenv import load_dotenv
from pathlib import Path

# ---------------- Setup ----------------
load_dotenv()
api_key = os.getenv("ENTSOE_API_KEY")
client = EntsoePandasClient(api_key=api_key)

start = pd.Timestamp("2021-03-22", tz="Europe/Zurich")
end   = pd.Timestamp("2025-01-01", tz="Europe/Zurich")

outdir = Path("data/raw")
outdir.mkdir(parents=True, exist_ok=True)

# ---------------- Fetch ----------------
print("Fetching planned generation outages for Switzerland (CH)...")

try:
    # ENTSO-E API call: unavailability of generation units
    outages = client.query_unavailability_of_generation_units(
        country_code="CH",
        start=start,
        end=end
    )

    if outages.empty:
        print("[warn] No outage data returned")
    else:
        # Reset index for tidy format
        outages = outages.reset_index()

        # Filter to "Planned" outages only (ex-ante valid)
        if "type" in outages.columns:
            outages = outages[outages["type"].str.contains("Planned", case=False, na=False)]

        # Save
        outpath = outdir / "ch_outages.csv"
        outages.to_csv(outpath, index=False)
        print(f"[ok] Saved {len(outages)} rows -> {outpath}")

except Exception as e:
    print(f"[fail] Could not fetch outages: {e}")
