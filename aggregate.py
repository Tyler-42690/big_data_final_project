"""Chunk-process the raw connections table and write a Parquet dataset
partitioned by `neuropil`.

Adds two columns:
- dominant_score: max probability across neurotransmitters (ties allowed)
- dominant_nt: comma-separated neurotransmitter name(s) at the max

This is intentionally a top-level script (not part of the `app` package)
so the project can keep `src/` as the single package source.
"""

from __future__ import annotations

from typing import List, Optional, cast
import os
import argparse
import logging
import shutil

import pyarrow as pa
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
import polars as pl

logging.basicConfig(
    filename="output.log",
    filemode="a",
    level=logging.WARNING,
    format="%(asctime)s - %(levelname)s - %(message)s",
)


NEURO_COLS: List[str] = [
    "gaba_avg",
    "ach_avg",
    "glut_avg",
    "oct_avg",
    "ser_avg",
    "da_avg",
]

NEURO_LABELS: dict[str, str] = {
    "gaba_avg": "GABA",
    "ach_avg": "Acetylcholine",
    "glut_avg": "Glutamate",
    "oct_avg": "Octopamine",
    "ser_avg": "Serotonin",
    "da_avg": "Dopamine",
}


def _add_dominant_columns(df: pl.DataFrame) -> pl.DataFrame:
    missing = [c for c in NEURO_COLS if c not in df.columns]
    if missing:
        raise ValueError(f"Missing columns: {missing}")

    df = df.with_columns(pl.max_horizontal([pl.col(c) for c in NEURO_COLS]).alias("dominant_score"))

    dominant_nt_expr = (
        pl.concat_list(
            [
                pl.when(pl.col(c) == pl.col("dominant_score"))
                .then(pl.lit(NEURO_LABELS.get(c, c)))
                .otherwise(None)
                for c in NEURO_COLS
            ]
        )
        .list.drop_nulls()
        .list.join(",")
        .fill_null("")
        .alias("dominant_nt")
    )

    return df.with_columns(dominant_nt_expr)


def write_partitioned_parquet_by_neuropil(
    feather_path: str,
    output_dir: str = "data/aggregates",
    max_rows: Optional[int] = None,
    dataset_dirname: str = "connections_with_dominant_nt_by_neuropil",
    drop_prob_cols: bool = False,
) -> str:
    """Chunk-process IPC/Feather and write a Parquet dataset partitioned by neuropil.

    Output layout:
      {output_dir}/{dataset_dirname}/neuropil=<value>/part-*.parquet

    Returns the dataset root directory.
    """

    if not os.path.exists(feather_path):
        raise FileNotFoundError(f"Feather file not found: {feather_path}")

    os.makedirs(output_dir, exist_ok=True)
    dataset_root = os.path.join(output_dir, dataset_dirname)
    if os.path.exists(dataset_root):
        shutil.rmtree(dataset_root)
    os.makedirs(dataset_root, exist_ok=True)

    total_written = 0
    file_index = 0

    print(f"Chunk-reading {feather_path} and writing partitioned Parquet dataset to {dataset_root}...")
    print("Partitioning: neuropil=<value>/part-*.parquet")

    with open(feather_path, "rb") as f:
        reader = ipc.open_file(f)
        num_batches = reader.num_record_batches
        print(f"Record batches in file: {num_batches}")

        for i in range(num_batches):
            if max_rows is not None and total_written >= max_rows:
                break

            batch = reader.get_batch(i)
            if max_rows is not None:
                remaining = max_rows - total_written
                if remaining <= 0:
                    break
                if batch.num_rows > remaining:
                    batch = batch.slice(0, remaining)

            table = pa.Table.from_batches([batch])
            
            df = cast(pl.DataFrame, pl.from_arrow(table))
            df = _add_dominant_columns(df)
            if drop_prob_cols:
                df = df.drop(NEURO_COLS)

            out_table = df.to_arrow()
            pq.write_to_dataset(
                out_table,
                root_path=dataset_root,
                partition_cols=["neuropil"],
                compression="snappy",
                basename_template=f"part-{file_index:06d}-{{i}}.parquet",
            )
            file_index += 1

            total_written += out_table.num_rows

            if (i + 1) % 10 == 0 or i == num_batches - 1:
                print(f"  wrote {total_written} rows (batch {i + 1}/{num_batches})")

    print(f"✓ Finished. Total rows written: {total_written}")
    return dataset_root


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Write a Parquet dataset partitioned by neuropil with dominant neurotransmitter columns"
    )
    p.add_argument("--input", "-i", default="data/raw/proofread_connections_783.feather", help="Path to input Feather file")
    p.add_argument("--output-dir", "-o", default="data/aggregates", help="Directory to write augmented data")
    p.add_argument("--max-rows", type=int, default=None, help="Optional maximum rows to read from the file")
    p.add_argument(
        "--dataset-dirname",
        default="connections_with_dominant_nt_by_neuropil",
        help="Output dataset folder name",
    )

    prob_group = p.add_mutually_exclusive_group()
    prob_group.add_argument(
        "--drop-prob-cols",
        action="store_true",
        help="Drop neurotransmitter probability columns from the output (keeps dominant_nt and dominant_score)",
    )
    prob_group.add_argument(
        "--keep-prob-cols",
        action="store_true",
        help="Keep neurotransmitter probability columns in the output (default)",
    )

    return p.parse_args()


def main() -> None:
    args = _parse_args()
    write_partitioned_parquet_by_neuropil(
        args.input,
        output_dir=args.output_dir,
        max_rows=args.max_rows,
        dataset_dirname=args.dataset_dirname,
        drop_prob_cols=args.drop_prob_cols,
    )


if __name__ == "__main__":
    main()
