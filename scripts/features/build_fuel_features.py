import pandas as pd
from pathlib import Path

TZ = "Europe/Zurich"

def build_fuel_features(rawdir="data/raw", outdir="data/processed"):
    rawdir, outdir = Path(rawdir), Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    files = {
        "ttf_gas": rawdir / "gas_bloomberg.csv",
        "eua_co2": rawdir / "carbon_bloomberg.csv",
    }

    all_features = []

    for name, path in files.items():
        if not path.exists():
            print(f"[skip] {name}: {path} not found")
            continue

        try:
            # Force ; as separator
            df = pd.read_csv(path, sep=";")

            # Standardize column names
            df.columns = [c.strip().lower() for c in df.columns]
            if "deliverydate" in df.columns:
                df = df.rename(columns={"deliverydate": "date"})
            elif "date" in df.columns:
                pass
            else:
                df.rename(columns={df.columns[0]: "date"}, inplace=True)

            if "price" not in df.columns:
                df.rename(columns={df.columns[-1]: "price"}, inplace=True)

            # Parse dates
            df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
            df = df.dropna(subset=["date", "price"]).sort_values("date")

            # Forward fill missing days (weekends, holidays)
            full_days = pd.date_range(df["date"].min(), df["date"].max(), freq="D")
            df = df.set_index("date").reindex(full_days).rename_axis("date").reset_index()
            df["price"] = df["price"].ffill()

            # Expand to hourly granularity
            # Expand to hourly granularity
            df = df.loc[df.index.repeat(24)].reset_index(drop=True)
            df["delivery_start_local"] = pd.date_range(
                start=df["date"].min(),
                periods=len(df),
                freq="h",
                tz=TZ
)


            # Keep tidy
            colname = f"{name}_eur"
            df = df[["delivery_start_local", "price"]].rename(columns={"price": colname})
            all_features.append(df)

            print(f"[ok] {name}: {len(df)} rows processed")

        except Exception as e:
            print(f"[fail] {name}: {e}")

    if all_features:
        out_df = all_features[0]
        for df in all_features[1:]:
            out_df = pd.merge(out_df, df, on="delivery_start_local", how="outer")

        out_df = out_df.sort_values("delivery_start_local")

        out = outdir / "fuels_features.csv"
        out_df.to_csv(out, index=False)
        print(f"[ok] Saved fuels features -> {out} ({len(out_df)} rows)")
    else:
        print("[fail] No fuels processed")

if __name__ == "__main__":
    build_fuel_features()
