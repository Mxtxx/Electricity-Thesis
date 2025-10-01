import pandas as pd
from pathlib import Path

def build_congestion_features(
    flow_file="data/processed/flow_features_all.csv",
    ntc_file="data/processed/ntc_features_all.csv",
    outdir="data/processed"
):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Load flow & ntc features
    flows = pd.read_csv(flow_file, parse_dates=["delivery_start_local"])
    ntc   = pd.read_csv(ntc_file, parse_dates=["delivery_start_local"])

    # Merge on time, border, direction
    merged = pd.merge(
        flows,
        ntc,
        on=["delivery_start_local", "border", "direction"],
        suffixes=("_flow", "_ntc"),
        how="inner"
    )

    # Build congestion ratio (with safe division)
    merged["congestion_ratio"] = merged["flow_mw"] / merged["ntc_mw"]
    merged["congestion_ratio"] = merged["congestion_ratio"].clip(-1.5, 1.5)  # avoid extreme spikes

    # Keep tidy columns
    keep = [
        "delivery_start_local", "border", "direction",
        "flow_mw", "flow_lag24",
        "ntc_mw", "ntc_lag24",
        "congestion_ratio"
    ]
    merged = merged[keep].sort_values(["delivery_start_local", "border", "direction"])

    # Save to CSV
    out = outdir / "congestion_features_all.csv"
    merged.to_csv(out, index=False)
    print(f"[ok] Saved congestion features -> {out} ({len(merged)} rows)")

if __name__ == "__main__":
    build_congestion_features()
