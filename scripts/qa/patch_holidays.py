import pandas as pd
import holidays
from pathlib import Path

def patch_holidays(infile="data/processed/master_dataset_clean.csv",
                   outfile="data/processed/master_dataset_patched.csv"):
    #load dataset
    df = pd.read_csv(infile, low_memory=False)
    print(f"[info] loaded dataset: {df.shape}")

    #force convert delivery_start_local to datetime
    df["delivery_start_local"] = pd.to_datetime(df["delivery_start_local"], errors="coerce", utc=True)
    df["delivery_start_local"] = df["delivery_start_local"].dt.tz_convert("Europe/Zurich")

    #create swiss holidays list for all years in dataset
    years = range(df["delivery_start_local"].dt.year.min(), df["delivery_start_local"].dt.year.max() + 1)
    ch_holidays = holidays.Switzerland(years=years)

    #extract date only (drop time part)
    df["date_only"] = df["delivery_start_local"].dt.date

    #check if the date is a swiss holiday
    df["is_holiday"] = df["date_only"].isin(ch_holidays).astype(int)

    #drop helper column
    df = df.drop(columns=["date_only"])

    #save patched dataset
    df.to_csv(outfile, index=False)
    print(f"[ok] patched holidays saved -> {outfile}")

    #quick sanity check: how many holidays
    print(df["is_holiday"].value_counts())

if __name__ == "__main__":
    patch_holidays()
