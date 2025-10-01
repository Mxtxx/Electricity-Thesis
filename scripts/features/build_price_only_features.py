import pandas as pd
from pathlib import Path

TZ = "Europe/Zurich"

def build_price_only_features(
    infile="data/raw/ch_day_ahead_prices.csv",
    outdir="data/processed"
):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Load Swiss day-ahead prices
    df = pd.read_csv(infile, parse_dates=[0])
    df.columns = ["delivery_start_local", "price"]

    # Ensure timezone-aware & sorted
    df["delivery_start_local"] = pd.to_datetime(df["delivery_start_local"], utc=True).dt.tz_convert(TZ)
    df = df.sort_values("delivery_start_local")

    # Build autoregressive lags (short, daily, weekly)
    for lag in [1, 24, 48, 168]:
        df[f"lag_{lag}h"] = df["price"].shift(lag)

    # Rolling mean & volatility
    df["rolling_mean_24h"] = df["price"].shift(1).rolling(window=24).mean()
    df["rolling_std_24h"]  = df["price"].shift(1).rolling(window=24).std()

    df["rolling_mean_168h"] = df["price"].shift(1).rolling(window=168).mean()
    df["rolling_std_168h"]  = df["price"].shift(1).rolling(window=168).std()

    # Price spreads (Δ vs. lagged values)
    df["spread_1d"] = df["price"] - df["lag_24h"]
    df["spread_7d"] = df["price"] - df["lag_168h"]

    # Drop rows with missing values
    df = df.dropna()

    # Save processed features
    outpath = outdir / "price_only_features.csv"
    df.to_csv(outpath, index=False)
    print(f"[ok] Price-only features saved -> {outpath} ({len(df)} rows, {df.shape[1]} cols)")

if __name__ == "__main__":
    build_price_only_features()
