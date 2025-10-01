import pandas as pd
from pathlib import Path
import holidays

TZ = "Europe/Zurich"

def build_calendar_features(start="2021-03-22", end="2024-12-31", outdir="data/processed"):
    outdir = Path(outdir)
    outdir.mkdir(parents=True, exist_ok=True)

    # Build hourly timeline
    hours = pd.date_range(start, end, freq="h", tz=TZ)
    df = pd.DataFrame({"delivery_start_local": hours})

    # Calendar basics
    df["hour"] = df["delivery_start_local"].dt.hour
    df["dayofweek"] = df["delivery_start_local"].dt.dayofweek  # 0=Mon, 6=Sun
    df["is_weekend"] = df["dayofweek"].isin([5, 6]).astype(int)
    df["month"] = df["delivery_start_local"].dt.month
    df["season"] = ((df["month"] % 12 + 3) // 3)  # 1=Winter, 2=Spring, 3=Summer, 4=Autumn

    # DST: check offset difference against UTC
    df["is_dst"] = (
        df["delivery_start_local"].dt.tz_convert("UTC").dt.hour
        != df["delivery_start_local"].dt.hour
    ).astype(int)

    # Swiss holidays
    ch_holidays = holidays.Switzerland(years=range(2021, 2025))
    df["date_only"] = df["delivery_start_local"].dt.normalize()
    df["is_holiday"] = df["date_only"].isin(ch_holidays).astype(int)
    df = df.drop(columns=["date_only"])

    # Save
    outpath = outdir / "calendar_features.csv"
    df.to_csv(outpath, index=False)
    print(f"[ok] Calendar features saved -> {outpath} ({len(df)} rows)")

if __name__ == "__main__":
    build_calendar_features()
