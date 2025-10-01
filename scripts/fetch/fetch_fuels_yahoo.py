import pandas as pd
import yfinance as yf
from pathlib import Path

# ---------------- Setup ----------------
symbols = {
    "ttf_gas": "TTF=F",   # Dutch TTF Gas Front-Month
    "eua_co2": "ECF=F"     # Carbon Emissions Futures (try C0=F, fallback to manual export)
}

start = "2021-03-22"
end   = "2024-12-31"

outdir = Path("data/raw/fuels")
outdir.mkdir(parents=True, exist_ok=True)

# ---------------- Fetch Loop ----------------
for name, ticker in symbols.items():
    print(f"Fetching {name} ({ticker}) from Yahoo Finance...")
    try:
        df = yf.download(
            ticker,
            start=start,
            end=end,
            interval="1d",
            progress=False
        )

        if df.empty:
            print(f"[warn] {name}: no data returned")
            continue

        # Use Adj Close if available, otherwise Close
        col = "Adj Close" if "Adj Close" in df.columns else "Close"
        df = df[[col]].rename(columns={col: "price"})
        df = df.reset_index().rename(columns={"Date": "date"})
        df["date"] = pd.to_datetime(df["date"]).dt.tz_localize("UTC").dt.tz_convert("Europe/Zurich")

        df = df[["date", "price"]]
        outpath = outdir / f"{name}_daily_yahoo.csv"
        df.to_csv(outpath, index=False)
        print(f"[ok] {name}: {len(df)} rows -> {outpath}")

    except Exception as e:
        print(f"[fail] {name}: {e}")
