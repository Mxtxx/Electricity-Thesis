import pandas as pd
from pathlib import Path

def build_fuel_features(rawdir="data/raw/fuels", outdir="data/processed"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    symbols = {
    "ttf_gas": "ttf_gas_daily.csv",
    "eua_co2": "eua_co2_daily.csv"
}


    for name, fname in symbols.items():
        infile = Path(rawdir) / fname
        try:
            df = pd.read_csv(infile, parse_dates=["date"])
        except Exception as e:
            print(f"[fail] Could not read {name}: {e}")
            continue

        if df.empty or "price" not in df.columns:
            print(f"[fail] {name}: missing data or price column")
            continue

        df = df.set_index("date").sort_index()

        # Forward-fill to hourly frequency
        df = df.resample("h").ffill()

        # Add lags
        df[f"{name}_lag24"] = df["price"].shift(24)
        df[f"{name}_lag168"] = df["price"].shift(168)

        df = df.reset_index().rename(columns={"date": "delivery_start_local", "price": f"{name}_price"})

        outpath = outdir / f"{name}_features.csv"
        df.to_csv(outpath, index=False)
        print(f"[ok] {name} features saved -> {outpath} ({len(df)} rows)")

if __name__ == "__main__":
    build_fuel_features()
