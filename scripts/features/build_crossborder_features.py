import pandas as pd
from pathlib import Path

#configuration
rawdir = Path("data/raw")
outdir = Path("data/processed")
outdir.mkdir(parents=True, exist_ok=True)

#cross-border links (always CH side involved)
links = [
    ("CH", "DE_LU"),
    ("CH", "FR"),
    ("CH", "IT_NORD")
]

#functions
def build_features(country_from, country_to):
    # File paths
    flow_file_fwd = rawdir / f"flow_{country_from.lower()}_{country_to.lower()}.csv"
    flow_file_rev = rawdir / f"flow_{country_to.lower()}_{country_from.lower()}.csv"
    ntc_file     = rawdir / f"ntc_{country_from.lower()}_{country_to.lower()}.csv"

    if not (flow_file_fwd.exists() and flow_file_rev.exists() and ntc_file.exists()):
        print(f"[skip] Missing raw files for {country_from}-{country_to}")
        return

    # Load raw CSVs and rename properly
    fwd = pd.read_csv(flow_file_fwd)
    fwd = fwd.rename(columns={fwd.columns[0]: "time", fwd.columns[1]: "flow_forward_mw"})

    rev = pd.read_csv(flow_file_rev)
    rev = rev.rename(columns={rev.columns[0]: "time", rev.columns[1]: "flow_reverse_mw"})

    ntc = pd.read_csv(ntc_file)
    ntc = ntc.rename(columns={ntc.columns[0]: "time", ntc.columns[1]: "ntc_mw"})

    # Merge
    df = pd.merge(fwd, rev, on="time", how="outer")
    df = pd.merge(df, ntc, on="time", how="outer").sort_values("time")

    # Congestion ratios
    df["flow_abs_mw"] = df["flow_forward_mw"].abs()
    df["congestion_ratio"] = df["flow_abs_mw"] / df["ntc_mw"]
    df["congestion_dummy"] = (df["congestion_ratio"] >= 0.95).astype(int)

    # Save
    out = outdir / f"crossborder_features_{country_from.lower()}_{country_to.lower()}.csv"
    df.to_csv(out, index=False)
    print(f"[ok] Saved features {country_from}-{country_to} -> {out}")

#main
def main():
    for c1, c2 in links:
        build_features(c1, c2)

if __name__ == "__main__":
    main()
