import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from pathlib import Path

def validate_master_dataset(infile="data/processed/master_dataset_clean.csv", sample_frac=0.02):
    print(f"[info] Loading dataset from {infile}...")
    df = pd.read_csv(infile, parse_dates=["delivery_start_local"], low_memory=False)

    # === Basic Info ===
    print("\n=== Basic Info ===")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    # === Missing Values ===
    print("\n=== Missing Values (Top 20) ===")
    print(df.isna().sum().sort_values(ascending=False).head(20))

    # === Summary Stats ===
    print("\n=== Summary Stats (numeric) ===")
    print(df.describe().transpose().head(15))

    # === Correlation (full dataset, numeric only) ===
    numeric_df = df.select_dtypes(include=["float64", "int64"])
    corr = numeric_df.corr()
    corr_out = Path("reports/qa")
    corr_out.mkdir(parents=True, exist_ok=True)
    corr.to_csv(corr_out / "correlation_matrix.csv")
    print(f"[ok] Correlation matrix saved -> {corr_out/'correlation_matrix.csv'}")

    # === Visuals on sample only ===
    print(f"\n[info] Sampling {sample_frac*100:.1f}% for visuals...")
    sample = df.sample(frac=sample_frac, random_state=42)

    # Missing values heatmap
    plt.figure(figsize=(12, 6))
    sns.heatmap(sample.isna(), cbar=False)
    plt.title("Missing Values Heatmap (sampled)")
    plt.tight_layout()
    plt.savefig(corr_out / "missing_heatmap.png")
    plt.close()
    print(f"[ok] Saved missing values heatmap -> {corr_out/'missing_heatmap.png'}")

    # Correlation heatmap (sample, numeric only)
    sample_numeric = sample.select_dtypes(include=["float64", "int64"])
    plt.figure(figsize=(10, 8))
    sns.heatmap(sample_numeric.corr(), cmap="coolwarm", center=0)
    plt.title("Correlation Heatmap (sampled, numeric only)")
    plt.tight_layout()
    plt.savefig(corr_out / "correlation_heatmap.png")
    plt.close()
    print(f"[ok] Saved correlation heatmap -> {corr_out/'correlation_heatmap.png'}")

if __name__ == "__main__":
    validate_master_dataset()
