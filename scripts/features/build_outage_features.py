import pandas as pd
from pathlib import Path

TZ = "Europe/Zurich"

def build_outage_features(infile="data/raw/ch_outages.csv", outdir="data/processed"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_csv(infile, parse_dates=["start", "end"])
    except Exception as e:
        print(f"[fail] Could not read {infile}: {e}")
        return

    if df.empty:
        print("[warn] Outage file is empty")
        return

    df["start"] = pd.to_datetime(df["start"], utc=True).dt.tz_convert(TZ)
    df["end"] = pd.to_datetime(df["end"], utc=True).dt.tz_convert(TZ)
    df["offline_mw"] = df["nominal_power"] - df["avail_qty"]

    start = df["start"].min().floor("h")
    end = df["end"].max().ceil("h")
    hours = pd.date_range(start, end, freq="h", tz=TZ)

    total_outage = pd.Series(0.0, index=hours)
    hydro_outage = pd.Series(0.0, index=hours)

    for _, row in df.iterrows():
        s, e, val = row["start"], row["end"], row["offline_mw"]
        is_hydro = "Hydro" in str(row["production_resource_psr_name"])
        if pd.notna(s) and pd.notna(e) and pd.notna(val):
            s_hour, e_hour = s.ceil("h"), e.floor("h")
            if s_hour <= e_hour:
                hours_range = pd.date_range(s_hour, e_hour, freq="h", tz=TZ)
                total_outage.loc[hours_range] += val
                if is_hydro:
                    hydro_outage.loc[hours_range] += val

    out = pd.DataFrame({
        "delivery_start_local": total_outage.index,
        "ch_outage_offline_mw": total_outage.values,
        "ch_hydro_outage_mw": hydro_outage.values
    })

    # Add lags
    out["ch_outage_offline_mw_lag24"] = out["ch_outage_offline_mw"].shift(24)
    out["ch_outage_offline_mw_lag168"] = out["ch_outage_offline_mw"].shift(168)
    out["ch_hydro_outage_mw_lag24"] = out["ch_hydro_outage_mw"].shift(24)
    out["ch_hydro_outage_mw_lag168"] = out["ch_hydro_outage_mw"].shift(168)

    outpath = outdir / "ch_outage_features.csv"
    out.to_csv(outpath, index=False)
    print(f"[ok] Outage features saved -> {outpath} ({len(out)} rows)")

if __name__ == "__main__":
    build_outage_features()
