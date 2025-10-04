import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def validate_master_dataset(
    infile="data/processed/master_dataset.csv",
    report_dir="reports/qa",
    sample_frac=0.02
):
    # make sure report directory exists
    os.makedirs(report_dir, exist_ok=True)

    print(f"[info] loading dataset from {infile}...")
    df = pd.read_csv(infile, parse_dates=["delivery_start_local"], low_memory=False)
    print(f"[info] validating dataset: {df.shape}")

    # === basic info ===
    print("\n=== Basic Info ===")
    print(f"Shape: {df.shape}")
    print(f"Columns: {list(df.columns)}")

    # === missing values ===
    print("\n=== Missing Values (Top 20) ===")
    print(df.isna().sum().sort_values(ascending=False).head(20))

    # === summary stats for numeric cols ===
    print("\n=== Summary Stats (numeric) ===")
    print(df.describe().transpose().head(15))

    # save numeric correlation matrix to csv
    numeric_cols = df.select_dtypes(include=["number"])
    corr_matrix = numeric_cols.corr()
    corr_out = os.path.join(report_dir, "correlation_matrix.csv")
    corr_matrix.to_csv(corr_out)
    print(f"[ok] correlation matrix saved -> {corr_out}")

    # === plots (on sample for speed) ===
    print(f"[info] sampling {sample_frac*100:.1f}% for visuals...")
    sample = df.sample(frac=sample_frac, random_state=42)

    # missing values heatmap
    plt.figure(figsize=(12, 6))
    sns.heatmap(sample.isna(), cbar=False)
    plt.title("Missing Values Heatmap (sample)")
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, "missing_heatmap.png"))
    plt.close()
    print(f"[ok] saved missing values heatmap -> {report_dir}/missing_heatmap.png")

    # correlation heatmap (numeric only)
    plt.figure(figsize=(12, 10))
    numeric_sample = sample.select_dtypes(include=["number"])  # only numeric columns
    sns.heatmap(numeric_sample.corr(), cmap="coolwarm", center=0)
    plt.title("Correlation Heatmap (numeric only, sample)")
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, "correlation_heatmap.png"))
    plt.close()
    print(f"[ok] saved correlation heatmap -> {report_dir}/correlation_heatmap.png")


if __name__ == "__main__":
    validate_master_dataset()
