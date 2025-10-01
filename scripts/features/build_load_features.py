import pandas as pd
from pathlib import Path

TZ = "Europe/Zurich"

def _to_local(ts):
    # Force UTC parsing, then convert to Swiss time
    s = pd.to_datetime(ts, errors="coerce", utc=True)
    return s.dt.tz_convert(TZ)

def _gct_asof(delivery_ts, zone="CH"):
    # For load forecasts, assume DA auction cutoff: D-1 11:00 local (CH) or 12:00 default
    cutoff = {"CH": 11, "default": 12}
    h = cutoff.get(zone, cutoff["default"])
    dminus1 = (delivery_ts.dt.tz_convert(TZ).dt.normalize() - pd.Timedelta(days=1))
    return dminus1 + pd.Timedelta(hours=h)

def build_load_features(rawdir="data/raw", outdir="data/processed"):
    rawdir, outdir = Path(rawdir), Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    try:
        infile = rawdir / "ch_load_forecast.csv"
        df = pd.read_csv(infile)

        # Expect first col = datetime, second = forecast
        tcol = df.columns[0]
        vcol = df.columns[1] if len(df.columns) > 1 else "forecast_load"
        df = df.rename(columns={tcol: "time", vcol: "forecast_load"})

        df["time"] = _to_local(df["time"])
        df = df.set_index("time").sort_index()

        # Reindex to full hourly grid
        full_hours = pd.date_range(df.index.min(), df.index.max(), freq="h", tz=TZ)
        df = df.reindex(full_hours)

        # Ex-ante feature: lag 24h and lag 168h
        df["forecast_load_lag24"] = df["forecast_load"].shift(24)
        df["forecast_load_lag168"] = df["forecast_load"].shift(168)

        # Add audit columns
        df.index.name = "delivery_start_local"
        df = df.reset_index()
        df["asof_local"] = _gct_asof(df["delivery_start_local"], "CH")

        # Keep tidy output
        keep = [
            "delivery_start_local", "asof_local",
            "forecast_load", "forecast_load_lag24", "forecast_load_lag168"
        ]
        df = df[keep].sort_values("delivery_start_local")

        out = outdir / "load_features_exante.csv"
        df.to_csv(out, index=False)
        print(f"[ok] Load: {len(df)} rows -> {out}")

    except Exception as e:
        print(f"[fail] Load features: {e}")

if __name__ == "__main__":
    build_load_features()
