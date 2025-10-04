import pandas as pd
import numpy as np
from pathlib import Path
import holidays

def aggregate_master_dataset(
    infile="data/processed/master_dataset.csv",
    outfile="data/processed/master_dataset_agg.csv",
    chunksize=500_000
):
    #paths
    infile, outfile = Path(infile), Path(outfile)
    print(f"[info] streaming dataset from {infile}...")

    #swiss holidays (extend years as needed)
    ch_holidays = holidays.Switzerland(years=range(2020, 2030))

    #we will store per-chunk hourly aggregates here
    results = []

    #read large CSV in chunks to avoid OOM
    for i, chunk in enumerate(pd.read_csv(infile, chunksize=chunksize, low_memory=False)):
        print(f"[info] processing chunk {i+1}")

        #--- 1) ensure timestamp is timezone-aware Europe/Zurich ---
        #convert parsed strings to datetime with UTC, then convert to CH local time
        chunk["delivery_start_local"] = pd.to_datetime(
            chunk["delivery_start_local"], errors="coerce", utc=True
        ).dt.tz_convert("Europe/Zurich")

        #--- 2) coerce numeric-like object columns (e.g. '18,73') to floats ---
        obj_cols = [c for c in chunk.columns if chunk[c].dtype == "object" and c != "delivery_start_local"]
        for c in obj_cols:
            #replace comma-decimal with dot, then try numeric; keep NaN on failure
            #this preserves true categoricals (they remain NaN after coercion and will be "first" in agg)
            chunk[c] = pd.to_numeric(
                chunk[c].astype(str).str.replace(",", ".", regex=False),
                errors="coerce"
            )

        #--- 3) choose aggregation per column: numeric -> mean, otherwise -> first ---
        agg_spec = {}
        for c in chunk.columns:
            if c == "delivery_start_local":
                continue
            if pd.api.types.is_numeric_dtype(chunk[c]):
                agg_spec[c] = "mean"
            else:
                agg_spec[c] = "first"

        #--- 4) aggregate to 1 row per hour for this chunk ---
        agg = chunk.groupby("delivery_start_local").agg(agg_spec).reset_index()
        results.append(agg)

    #--- 5) concatenate chunk aggregates & re-aggregate (in case hours spanned multiple chunks) ---
    df = pd.concat(results, ignore_index=True)
    df = df.groupby("delivery_start_local").mean(numeric_only=True).reset_index()

    print("[info] finished numeric aggregation, recomputing calendar features...")

    #--- 6) ensure datetime dtype & CH timezone again (safe guard) ---
    df["delivery_start_local"] = pd.to_datetime(df["delivery_start_local"], utc=True).dt.tz_convert("Europe/Zurich")

    #--- 7) rebuild calendar features deterministically from timestamp ---
    #hour-of-day, weekday, weekend, month, season
    df["hour"] = df["delivery_start_local"].dt.hour
    df["dayofweek"] = df["delivery_start_local"].dt.dayofweek
    df["is_weekend"] = df["dayofweek"].isin([5, 6]).astype(int)
    df["month"] = df["delivery_start_local"].dt.month
    df["season"] = (df["month"] % 12 + 3) // 3  # 1=winter,2=spring,3=summer,4=autumn

    #dst robustly: label is 'CEST' in DST, 'CET' otherwise
    df["is_dst"] = df["delivery_start_local"].dt.strftime("%Z").eq("CEST").astype(int)

    #holidays (Switzerland)
    df["date_only"] = df["delivery_start_local"].dt.date
    df["is_holiday"] = df["date_only"].isin(ch_holidays).astype(int)
    df = df.drop(columns=["date_only"])

    #--- 8) forward-fill daily fundamentals across 24h (avoid sparse daily stamps) ---
    for var in ["ttf_gas_price", "eua_co2_price"]:
        if var in df.columns:
            df[var] = df[var].ffill()

    #--- 9) optional cleanup: drop meaningless leftover helper columns if any slipped through ---
    #we aggregated numerics, so border/direction/asof columns should not be here; safeguard anyway:
    drop_like = [c for c in df.columns if "border" in c or "direction" in c or "asof" in c]
    if drop_like:
        df = df.drop(columns=drop_like)

    #--- 10) final sort + save ---
    df = df.sort_values("delivery_start_local")
    Path(outfile).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(outfile, index=False)
    print(f"[ok] saved aggregated dataset -> {outfile} ({len(df)} rows, {len(df.columns)} cols)")

if __name__ == "__main__":
    aggregate_master_dataset()
