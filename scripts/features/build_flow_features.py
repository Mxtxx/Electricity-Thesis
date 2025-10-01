import pandas as pd
from pathlib import Path

TZ = "Europe/Zurich"

# Define interconnectors (CH with neighbors)
LINKS = [
    ("CH", "DE_LU"),
    ("CH", "FR"),
    ("CH", "IT_NORD"),
]

def _to_local(ts):
    s = pd.to_datetime(ts, errors="coerce", utc=True)
    return s.dt.tz_convert(TZ)

def build_flow_features(rawdir="data/raw", outdir="data/processed"):
    rawdir, outdir = Path(rawdir), Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    all_blocks = []

    for c1, c2 in LINKS:
        for src, dst in [(c1, c2), (c2, c1)]:
            try:
                fname = f"flow_{src.lower()}_{dst.lower()}.csv"
                path = rawdir / fname
                if not path.exists():
                    print(f"[skip] {fname} not found")
                    continue

                df = pd.read_csv(path)

                # Expect first col = datetime, second = flow value
                tcol = df.columns[0]
                vcol = df.columns[1] if len(df.columns) > 1 else "flow_mw"
                df = df.rename(columns={tcol: "time", vcol: "flow_mw"})
                df["time"] = _to_local(df["time"])
                df = df.set_index("time").sort_index()

                # Reindex to full hourly grid
                full_hours = pd.date_range(df.index.min(), df.index.max(), freq="h", tz=TZ)
                df = df.reindex(full_hours)

                # Add ex-ante lag (24h)
                df["flow_lag24"] = df["flow_mw"].shift(24)

                # Reset index for tidy format
                df.index.name = "delivery_start_local"
                df = df.reset_index()

                # Add metadata
                df["border"] = f"{c1}_{c2}"
                df["direction"] = f"{src}->{dst}"

                all_blocks.append(df)

                print(f"[ok] {src}->{dst}: {len(df)} rows processed")

            except Exception as e:
                print(f"[fail] {src}->{dst}: {e}")

    # === Concatenate all flows into one tidy file ===
    if all_blocks:
        out_df = pd.concat(all_blocks, ignore_index=True)
        out_df = out_df.sort_values(["delivery_start_local", "border", "direction"])

        out = outdir / "flow_features_all.csv"
        out_df.to_csv(out, index=False)
        print(f"[ok] Saved all flows -> {out} ({len(out_df)} rows)")
    else:
        print("[fail] No flows processed")

if __name__ == "__main__":
    build_flow_features()
