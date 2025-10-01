import pandas as pd
from pathlib import Path

TZ = "Europe/Zurich"

def build_price_features(rawdir="data/raw", outdir="data/processed"):
    rawdir, outdir = Path(rawdir), Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    files = {
        "price_ch": rawdir / "ch_day_ahead_prices.csv",
        "price_de_lu": rawdir / "de_lu_day_ahead_prices.csv",
        "price_fr": rawdir / "fr_day_ahead_prices.csv",
        "price_it_nord": rawdir / "it_nord_day_ahead_prices.csv",
    }

    all_dfs = []

    for colname, path in files.items():
        if not path.exists():
            print(f"[skip] {colname}: {path} not found")
            continue

        try:
            df = pd.read_csv(path, parse_dates=[0])  # first col = datetime
            df.columns = ["delivery_start_local", colname]
            df["delivery_start_local"] = pd.to_datetime(df["delivery_start_local"], utc=True).dt.tz_convert(TZ)
            all_dfs.append(df)
            print(f"[ok] {colname}: {len(df)} rows loaded")

        except Exception as e:
            print(f"[fail] {colname}: {e}")

    if not all_dfs:
        print("[fail] No price files processed")
        return

    # Merge all price series on delivery_start_local
    prices = all_dfs[0]
    for df in all_dfs[1:]:
        prices = pd.merge(prices, df, on="delivery_start_local", how="outer")

    prices = prices.sort_values("delivery_start_local")

    # Save consolidated prices
    out = outdir / "day_ahead_prices.csv"
    prices.to_csv(out, index=False)
    print(f"[ok] Saved consolidated prices -> {out} ({prices.shape[0]} rows, {prices.shape[1]} cols)")

if __name__ == "__main__":
    build_price_features()
