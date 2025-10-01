import pandas as pd
from pathlib import Path
from functools import reduce

TZ = "Europe/Zurich"

def build_master_dataset(processed_dir="data/processed", outdir="data/final"):
    processed_dir, outdir = Path(processed_dir), Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Define files to merge
    files = {
        "calendar": "calendar_features.csv",
        "price_only": "price_only_features.csv",
        "res": "ch_res_features_exante.csv",
        "hydro": "ch_hydro_features.csv",
        "load": "load_features_exante.csv",
        "fuels_gas": "ttf_gas_features.csv",
        "fuels_co2": "eua_co2_features.csv",
        "outages": "ch_outage_features.csv",
        "flows": "flow_features_all.csv",
        "ntc": "ntc_features_all.csv",
        "congestion": "congestion_features_all.csv"
    }

    dfs = []
    for name, fname in files.items():
        fpath = processed_dir / fname
        if not fpath.exists():
            print(f"[skip] {name}: {fname} not found")
            continue

        try:
            df = pd.read_csv(fpath)

            # --- Force datetime ---
            if "delivery_start_local" in df.columns:
                df["delivery_start_local"] = pd.to_datetime(
                    df["delivery_start_local"], errors="coerce", utc=True
                ).dt.tz_convert(TZ)

            dfs.append(df)
            print(f"[ok] {name}: {len(df)} rows loaded")
        except Exception as e:
            print(f"[fail] {name}: {e}")

    if not dfs:
        print("[fail] No datasets loaded, aborting.")
        return

    # Merge everything on delivery_start_local
    master = reduce(
        lambda left, right: pd.merge(left, right, on="delivery_start_local", how="outer"),
        dfs
    )

    # Sort and clean
    master = master.sort_values("delivery_start_local").reset_index(drop=True)

    # Drop rows where Swiss price (target) is missing
    if "price" in master.columns:
        master = master.dropna(subset=["price"])

    out = outdir / "master_dataset.csv"
    master.to_csv(out, index=False)
    print(f"[ok] Master dataset saved -> {out} ({master.shape[0]} rows, {master.shape[1]} cols)")

if __name__ == "__main__":
    build_master_dataset()
