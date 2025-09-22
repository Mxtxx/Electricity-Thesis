import pandas as pd
import requests
import time
from pathlib import Path

API = "https://historical-forecast-api.open-meteo.com/v1/forecast"
TZ = "Europe/Zurich"

ZONES = {
    "CH": (46.8, 8.3),
    "DE_LU": (51.2, 10.4),
    "FR": (46.6, 2.2),
    "IT_NORD": (45.5, 9.0),
}

HOURLY = ",".join([
    "shortwave_radiation", "direct_radiation", "diffuse_radiation",
    "wind_speed_80m", "wind_speed_120m", "cloud_cover"
])

def fetch_block(lat, lon, start, end):
    params = {
        "latitude": lat, "longitude": lon,
        "hourly": HOURLY,
        "models": "best_match",
        "timezone": "UTC",
        "start_date": start, "end_date": end
    }
    for attempt in range(3):  # retry up to 3 times
        try:
            r = requests.get(API, params=params, timeout=60)
            r.raise_for_status()
            return pd.DataFrame(r.json()["hourly"])
        except Exception as e:
            print(f"  [retry {attempt+1}] {e}")
            time.sleep(5)
    return pd.DataFrame()

def fetch_zone(zone, lat, lon, start, end):
    """Fetch weather forecasts in yearly chunks and concatenate"""
    start_date = pd.to_datetime(start)
    end_date = pd.to_datetime(end)

    all_df = []
    for year in range(start_date.year, end_date.year + 1):
        chunk_start = pd.Timestamp(f"{year}-01-01")
        chunk_end = pd.Timestamp(f"{year}-12-31")

        # clip to requested start/end
        if chunk_start < start_date:
            chunk_start = start_date
        if chunk_end > end_date:
            chunk_end = end_date

        if chunk_start > chunk_end:
            continue  # skip invalid ranges

        print(f"  fetching {zone} {chunk_start.date()}..{chunk_end.date()}")
        df = fetch_block(
            lat,
            lon,
            chunk_start.date().isoformat(),
            chunk_end.date().isoformat()
        )

        if not df.empty:
            df["time"] = pd.to_datetime(df["time"], utc=True).dt.tz_convert(TZ)
            df.insert(1, "zone", zone)
            all_df.append(df)

    return pd.concat(all_df, ignore_index=True) if all_df else pd.DataFrame()

def main(start="2021-03-22", end="2025-01-01", outdir="data/raw"):
    outdir = Path(outdir); outdir.mkdir(parents=True, exist_ok=True)
    for zone, (lat, lon) in ZONES.items():
        print(f"[fetch] {zone} {start}..{end}")
        df = fetch_zone(zone, lat, lon, start, end)
        if not df.empty:
            out = outdir / f"{zone.lower()}_weather_openmeteo.csv"
            df.to_csv(out, index=False)
            print(f"[ok] {zone}: {len(df)} rows saved to {out}")
        else:
            print(f"[fail] {zone}: no data retrieved")

if __name__ == "__main__":
    main()

