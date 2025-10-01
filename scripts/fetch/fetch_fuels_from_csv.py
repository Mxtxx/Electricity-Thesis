import pandas as pd
from pathlib import Path

#get files
files = {
    "ttf_gas": "data/raw/gas_bloomberg.csv",
    "eua_co2": "data/raw/carbon_bloomberg.csv"
}

outdir = Path("data/raw/fuels")
outdir.mkdir(parents=True, exist_ok=True)

#process loop
for name, infile in files.items():
    print(f"Processing {name} from {infile}...")
    try:
        #load file with ; as separator
        df = pd.read_csv(infile, sep=";")

        #normalize column names
        df.columns = [c.lower() for c in df.columns]

        if name == "ttf_gas":
            df = df.rename(columns={"deliverydate": "date"})
        elif name == "eua_co2":
            df = df.rename(columns={"date": "date"})

        #ensure we only keep date + price
        if "price" not in df.columns:
            raise ValueError(f"Expected 'price' column in {infile}, got {df.columns}")

        df["date"] = pd.to_datetime(df["date"], errors="coerce", dayfirst=True)
        df = df.dropna(subset=["date", "price"])
        df = df[["date", "price"]]

        #save standardized CSV
        outpath = outdir / f"{name}_daily.csv"
        df.to_csv(outpath, index=False)
        print(f"[ok] {name}: {len(df)} rows -> {outpath}")

    except Exception as e:
        print(f"[fail] {name}: {e}")
