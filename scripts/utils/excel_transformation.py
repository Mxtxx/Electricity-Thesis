import pandas as pd
from pathlib import Path

# Load Excel file
xls = pd.ExcelFile("data/raw/Commodities_2025-05-19.xlsx")  # adjust path if needed

# === Carbon ===
carbon = pd.read_excel(xls, sheet_name="Carbon")
carbon = carbon.rename(columns={"Date": "date", "EUA_Spot": "price"})
carbon["date"] = pd.to_datetime(carbon["date"])
carbon = carbon[["date", "price"]].dropna()

# === Gas ===
gas = pd.read_excel(xls, sheet_name="Gas")
gas = gas.rename(columns={"DeliveryDate": "date", "Gas": "price"})
gas["date"] = pd.to_datetime(gas["date"])
gas = gas[["date", "price"]].dropna()

# === Save in repo structure ===
outdir = Path("data/raw/fuels")
outdir.mkdir(parents=True, exist_ok=True)

carbon.to_csv(outdir / "eua_co2_daily.csv", index=False)
gas.to_csv(outdir / "ttf_gas_daily.csv", index=False)

print("[ok] Saved:", list(outdir.glob("*.csv")))
