import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import os

def validate_master_agg(
    infile="data/processed/master_dataset_agg.csv",
    report_dir="reports/qa_agg"
):
    #make reports dir
    os.makedirs(report_dir, exist_ok=True)

    print(f"[info] loading aggregated dataset from {infile}...")
    df = pd.read_csv(infile, parse_dates=["delivery_start_local"])
    print(f"[info] shape: {df.shape}")

    #basic info
    print("\n=== columns ===")
    print(list(df.columns))

    print("\n=== top-20 missing ===")
    print(df.isna().sum().sort_values(ascending=False).head(20))

    #monotonic time & duplicates
    is_mono = df["delivery_start_local"].is_monotonic_increasing
    n_dups = df["delivery_start_local"].duplicated().sum()
    print(f"\n[info] time monotonic: {is_mono}, duplicate timestamps: {n_dups}")

    #numeric-only describe
    desc = df.select_dtypes(include="number").describe().T
    desc.to_csv(os.path.join(report_dir, "describe_numeric.csv"))
    print(f"[ok] numeric describe saved -> {report_dir}/describe_numeric.csv")

    #missing heatmap
    plt.figure(figsize=(12, 6))
    sns.heatmap(df.isna(), cbar=False)
    plt.title("missing values heatmap (aggregated)")
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, "missing_heatmap_agg.png"))
    plt.close()
    print(f"[ok] saved -> {report_dir}/missing_heatmap_agg.png")

    #correlation heatmap (numeric only)
    num = df.select_dtypes(include="number")
    corr = num.corr()
    corr.to_csv(os.path.join(report_dir, "correlation_matrix_agg.csv"))
    plt.figure(figsize=(12, 10))
    sns.heatmap(corr, cmap="coolwarm", center=0)
    plt.title("correlation heatmap (aggregated, numeric only)")
    plt.tight_layout()
    plt.savefig(os.path.join(report_dir, "correlation_heatmap_agg.png"))
    plt.close()
    print(f"[ok] saved -> {report_dir}/correlation_heatmap_agg.png")

if __name__ == "__main__":
    validate_master_agg()
