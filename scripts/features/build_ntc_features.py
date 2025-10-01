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

def build_ntc_features(rawdir="data/raw", outdir="data/processed"):
    rawdir, outdir = Path(rawdir), Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    all_blocks = []

    for c1, c2 in LINKS:
        values = {}

        for src, dst in [(c1, c2), (c2, c1)]:
            fname = f"ntc_{src.lower()}_{dst.lower()}.csv"
            path = rawdir / fname
            if path.exists():
                df = pd.read_csv(path)

                # Expect first col = datetime, second = NTC value
                tcol = df.columns[0]
                vcol = df.columns[1] if len(df.columns) > 1 else "ntc_mw"
                df = df.rename(columns={tcol: "time", vcol: "ntc_mw"})
                df["time"] = _to_local(df["time"])
                df = df.set_index("time").sort_index()

                # Reindex to full hourly grid
                full_hours = pd.date_range(df.index.min(), df.index.max(), freq="h", tz=TZ)
                df = df.reindex(full_hours)

                # Add ex-ante lag (24h)
                df["ntc_lag24"] = df["ntc_mw"].shift(24)

                # Reset index
                df.index.name = "delivery_start_local"
                df = df.reset_index()

                # Metadata
                df["border"] = f"{c1}_{c2}"
                df["direction"] = f"{src}->{dst}"

                values[(src, dst)] = df
                print(f"[ok] {src}->{dst}: {len(df)} rows processed")

        # If only one direction exists → duplicate for reverse
        if (c1, c2) in values and (c2, c1) not in values:
            df_copy = values[(c1, c2)].copy()
            df_copy["direction"] = f"{c2}->{c1}"
            values[(c2, c1)] = df_copy
            print(f"[dup] Duplicated {c1}->{c2} as {c2}->{c1}")

        elif (c2, c1) in values and (c1, c2) not in values:
            df_copy = values[(c2, c1)].copy()
            df_copy["direction"] = f"{c1}->{c2}"
            values[(c1, c2)] = df_copy
            print(f"[dup] Duplicated {c2}->{c1} as {c1}->{c2}")

        all_blocks.extend(values.values())

    # === Concatenate all NTCs into one tidy file ===
    if all_blocks:
        out_df = pd.concat(all_blocks, ignore_index=True)
        out_df = out_df.sort_values(["delivery_start_local", "border", "direction"])

        out = outdir / "ntc_features_all.csv"
        out_df.to_csv(out, index=False)
        print(f"[ok] Saved all NTC -> {out} ({len(out_df)} rows)")
    else:
        print("[fail] No NTC processed")

if __name__ == "__main__":
    build_ntc_features()
