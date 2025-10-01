import pandas as pd
from pathlib import Path

def build_hydro_features(infile="data/raw/ch_hydro_generation_entsoe.csv", outdir="data/processed"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        df = pd.read_csv(infile, parse_dates=["index"])
    except Exception as e:
        print(f"[fail] Could not read {infile}: {e}")
        return

    if df.empty:
        print("[warn] Hydro generation file is empty")
        return

    df = df.rename(columns={"index": "delivery_start_local"})
    df = df.set_index("delivery_start_local").sort_index()

    # Add lags
    for col in df.columns:
        df[f"{col}_lag24"] = df[col].shift(24)
        df[f"{col}_lag168"] = df[col].shift(168)

    df = df.reset_index()

    outpath = outdir / "ch_hydro_features.csv"
    df.to_csv(outpath, index=False)
    print(f"[ok] Hydro features saved -> {outpath} ({len(df)} rows)")

if __name__ == "__main__":
    build_hydro_features()
