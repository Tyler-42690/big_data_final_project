"""Plotting functions for visualizing connection data."""
import polars as pl
import matplotlib.pyplot as plt

def plot_connections_per_neuropil(lf: pl.LazyFrame) -> None:
    """
    Plots the number of connections per neuropil as a bar chart.
    Expects a Polars DataFrame with the 'neuropil' column.
    """
    df = (
    lf.group_by("neuropil")
      .len()
      .sort("len", descending=True)
      .collect()
)

    plt.figure()
    plt.bar(df["neuropil"], df["len"])
    plt.xticks(rotation=90)
    plt.xlabel("Neuropil")
    plt.ylabel("Connection count")
    plt.title("Connections per Neuropil")
    plt.tight_layout()
    plt.show()

def plot_dominant_nt_distribution(lf: pl.LazyFrame) -> None:
    """
    Plots the dominant neurotransmitter distribution as a bar chart.
    Expects a Polars DataFrame with the 'dominant_nt' column.
    """
    df = (
    lf.group_by("dominant_nt")
      .len()
      .sort("len", descending=True)
      .collect()
)

    plt.figure()
    plt.bar(df["dominant_nt"], df["len"])
    plt.xlabel("Dominant neurotransmitter")
    plt.ylabel("Connection count")
    plt.title("Dominant Neurotransmitter Distribution")
    plt.tight_layout()
    plt.show()

def plot_avg_dominant_score_per_neurotransmitter(lf: pl.LazyFrame) -> None:
    """
    Plots the average dominant score per neurotransmitter as a bar chart.
    Expects a Polars DataFrame with the 'dominant_nt' column.
    """
    df = (
    lf.group_by("dominant_nt")
      .agg(pl.mean("dominant_score").alias("avg_score"))
      .sort("avg_score", descending=True)
      .collect()
)

    plt.figure()
    plt.bar(df["dominant_nt"], df["avg_score"])
    plt.xlabel("Dominant neurotransmitter")
    plt.ylabel("Average dominant probability")
    plt.title("Mean Dominant NT Probability")
    plt.tight_layout()
    plt.show()

def plot_neuropil_vs_neurotransmitter_heatmap(lf: pl.LazyFrame) -> None:
    """
    Plots a heatmap of neuropil vs dominant neurotransmitter.
    Expects a Polars DataFrame with 'neuropil' and 'dominant_nt' columns.
    """
    pivots = (
        lf.group_by(["neuropil", "dominant_nt"])
        .len()
        .collect()
        .pivot(
            values="len",
            index="neuropil",
            on="dominant_nt",  # <--- CHANGED from 'columns' to 'on'
            aggregate_function="first",
        )
        .fill_null(0)
    )

    matrix = pivots.drop("neuropil").to_numpy()
    neuropils = pivots["neuropil"].to_list()
    nts = pivots.columns[1:]

    plt.figure()
    plt.imshow(matrix, aspect="auto")
    plt.colorbar(label="Connection count")
    plt.xticks(range(len(nts)), nts, rotation=45)
    plt.yticks(range(len(neuropils)), neuropils)
    plt.xlabel("Dominant neurotransmitter")
    plt.ylabel("Neuropil")
    plt.title("Dominant NT by Neuropil")
    plt.tight_layout()
    plt.show()


def main() -> None:
    """Main function to run plotting routines."""
    
    DATASET_PATH = "data/aggregates/connections_with_dominant_nt_by_neuropil"

    lf = pl.scan_parquet(DATASET_PATH)
    plot_connections_per_neuropil(lf)
    plot_dominant_nt_distribution(lf)
    plot_avg_dominant_score_per_neurotransmitter(lf)
    plot_neuropil_vs_neurotransmitter_heatmap(lf)

if __name__ == "__main__":
    main()