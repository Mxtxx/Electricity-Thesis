# scripts/qa/clean_master_dataset.py

import pandas as pd
from pathlib import Path

def clean_master_dataset(
    infile="data/processed/master_dataset.csv",
    outfile="data/processed/master_dataset_clean.csv"
):
    print(f"[info] Loading dataset from {infile}...")
    df = pd.read_csv(infile, low_memory=False)

    # Ensure delivery_start_local is datetime with timezone
    df["delivery_start_local"] = pd.to_datetime(df["delivery_start_local"], errors="coerce", utc=True)
    df["delivery_start_local"] = df["delivery_start_local"].dt.tz_convert("Europe/Zurich")

    # === 1) Drop duplicate or irrelevant columns ===
    drop_cols = [c for c in df.columns if c.startswith("asof_local")]
    if drop_cols:
        print(f"[info] Dropping duplicate cols: {drop_cols}")
        df = df.drop(columns=drop_cols)

    # === 2) Fix DST indicator ===
    print("[info] Recomputing is_dst...")
    df["is_dst"] = df["delivery_start_local"].apply(lambda x: 1 if x.dst() != pd.Timedelta(0) else 0)

    # === 3) Handle missing values ===
    print("[info] Handling missing values...")

    # Fuels (ffill + bfill)
    for col in ["ttf_gas_price", "ttf_gas_lag24", "ttf_gas_lag168",
                "eua_co2_price", "eua_co2_lag24", "eua_co2_lag168"]:
        if col in df.columns:
            df[col] = df[col].ffill().bfill()

    # Weather (interpolate linearly)
    weather_cols = ["shortwave_radiation", "direct_radiation", "diffuse_radiation",
                    "wind_speed_80m", "wind_speed_120m", "cloud_cover",
                    "pv_proxy_wm2", "wind_proxy_unit"]
    for col in weather_cols:
        if col in df.columns:
            df[col] = df[col].interpolate(limit_direction="both")

    # Calendar (drop rows if missing, should be rare)
    calendar_cols = ["hour", "dayofweek", "is_weekend", "month", "season", "is_holiday"]
    df = df.dropna(subset=calendar_cols)

    # Lags: leave NaNs (normal for first days), or drop first week if you prefer
    min_time = df["delivery_start_local"].min()
    df = df[df["delivery_start_local"] >= (min_time + pd.Timedelta(days=7))]

    # === 4) Categorical encoding preparation ===
    cat_cols = ["border", "direction", "zone"]
    for col in cat_cols:
        if col in df.columns:
            df[col] = df[col].astype("category")

    # === Save cleaned dataset ===
    out = Path(outfile)
    out.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(out, index=False)
    print(f"[ok] Saved cleaned dataset -> {out} ({len(df)} rows, {df.shape[1]} cols)")


if __name__ == "__main__":
    clean_master_dataset()
