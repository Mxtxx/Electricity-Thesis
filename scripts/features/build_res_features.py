from pathlib import Path
import pandas as pd

TZ = "Europe/Zurich"

# DA auction (local) gate-closure times
GCT_LOCAL = {
    "CH": "11:00",
    "default": "12:00"
}

def _delivery_hours_for_day(d_local_date):
    s = pd.Timestamp(d_local_date, tz=TZ)
    e = s + pd.Timedelta(days=1)
    # robust to 23/25-hour DST days
    return pd.date_range(s, e, freq="h", inclusive="left")

def _gct_for_zone(zone: str, delivery_day_local: pd.Timestamp) -> pd.Timestamp:
    # as-of is D-1 at 11:00/12:00 local; we store it for audit / merging discipline
    cutoff = GCT_LOCAL.get(zone, GCT_LOCAL["default"])
    d_minus_1 = (delivery_day_local - pd.Timedelta(days=1)).date()
    return pd.Timestamp(f"{d_minus_1} {cutoff}", tz=TZ)

def _build_one_zone(infile: Path, outdir: Path):
    # ✅ FIX: load "time" column instead of ts_local/ts_utc
    df = pd.read_csv(infile, parse_dates=["time"])
    if df.empty:
        return

    # Recreate UTC and Local timestamps
    df["ts_utc"] = pd.to_datetime(df["time"], utc=True)
    df["ts_local"] = df["ts_utc"].dt.tz_convert(TZ)

    zone = df["zone"].iloc[0]
    df = df.sort_values("ts_local")

    # Ensure expected columns exist
    needed = [
        "shortwave_radiation","direct_radiation","diffuse_radiation",
        "wind_speed_80m","wind_speed_120m","cloud_cover"
    ]
    for c in needed:
        if c not in df.columns:
            df[c] = pd.NA

    # Build per-day delivery rows with ex-ante as-of tag
    start_day = df["ts_local"].dt.normalize().min()
    end_day   = (df["ts_local"].dt.normalize().max() + pd.Timedelta(days=1)).normalize()
    days = pd.date_range(start_day, end_day, freq="D", inclusive="left")

    blocks = []
    for d0 in days:
        delivery_hours = _delivery_hours_for_day(d0.date())
        # join weather (already hour-aligned by ts_local)
        take = df.set_index("ts_local").reindex(delivery_hours).reset_index()
        take.rename(columns={"index":"delivery_start_local"}, inplace=True)
        # Tag as-of (D-1 11:00/12:00 local) for audit & merges
        take["asof_local"] = _gct_for_zone(zone, d0)
        take["zone"] = zone
        blocks.append(take)

    out = pd.concat(blocks, ignore_index=True)

    # === Proxies (no extra model) ===
    # PV proxy: clip GHI at 0
    out["pv_proxy_wm2"] = out["shortwave_radiation"].clip(lower=0)

    # Wind proxy: v^3 using 120m if available else 80m; safe clip at 0; per-day normalization
    v = out["wind_speed_120m"].fillna(out["wind_speed_80m"])
    v = pd.to_numeric(v, errors="coerce").clip(lower=0)
    raw = v ** 3
    out["wind_proxy_raw"] = raw

    # Normalize by 99th percentile *within the same local day* to reduce storm/spike leverage
    out["date"] = out["delivery_start_local"].dt.date
    denom = out.groupby("date")["wind_proxy_raw"].transform(
        lambda s: s.quantile(0.99) if s.notna().any() else pd.NA
    )
    out["wind_proxy_unit"] = (out["wind_proxy_raw"] / denom).clip(upper=1.5)

    # Keep tidy columns for downstream merges (raw + proxies + audit)
    keep = [
        "delivery_start_local","asof_local","zone",
        "shortwave_radiation","direct_radiation","diffuse_radiation",
        "wind_speed_80m","wind_speed_120m","cloud_cover",
        "pv_proxy_wm2","wind_proxy_unit"
    ]
    out = out[keep].sort_values(["delivery_start_local","zone"])

    outdir.mkdir(parents=True, exist_ok=True)
    out_path = outdir / f"{zone.lower()}_res_features_exante.csv"
    out.to_csv(out_path, index=False)
    print(f"[ok] {zone}: wrote {len(out)} rows -> {out_path}")

def main(rawdir="data/raw", outdir="data/processed"):
    rawdir = Path(rawdir); outdir = Path(outdir)
    for infile in sorted(rawdir.glob("*_weather_openmeteo.csv")):
        try:
            _build_one_zone(infile, outdir)
        except Exception as e:
            print(f"[fail] {infile.name}: {e}")

if __name__ == "__main__":
    main()
