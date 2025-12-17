"""Summarize the Parquet Gold dataset (partitioned by neuropil).

This script reads the Parquet dataset produced by aggregate.py:
  data/aggregates/connections_with_dominant_nt_by_neuropil/

It groups by neuropil + neurotransmitter and produces the same histogram-style
summary used by the dashboard (so threshold queries are fast via suffix sums).

Output JSON schema matches summarize_data.py so it can be pushed into Neo4j and
served by /dataset/summary.
"""

from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, cast

import pyarrow as pa
import pyarrow.dataset as ds
import polars as pl


NEUROTRANSMITTERS = [
    "GABA",
    "Acetylcholine",
    "Glutamate",
    "Octopamine",
    "Serotonin",
    "Dopamine",
]


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Summarize the Parquet Gold dataset (partitioned by neuropil) for the dashboard"
    )
    p.add_argument(
        "--dataset-root",
        default="data/aggregates/connections_with_dominant_nt_by_neuropil",
        help="Root path of the Parquet dataset (Hive partitioned by neuropil)",
    )
    p.add_argument(
        "--out-json",
        default="data/summary/flywire_summary_from_parquet.json",
        help="Where to write the summary JSON",
    )
    p.add_argument(
        "--max-rows",
        type=int,
        default=None,
        help="Optional cap for quick tests (first N rows encountered during scan)",
    )
    p.add_argument(
        "--batch-rows",
        type=int,
        default=250_000,
        help="Approximate batch size for scanning (higher is faster but uses more memory)",
    )
    return p.parse_args()


def _ensure_dir(path: str) -> None:
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)


def main() -> None:
    args = _parse_args()

    dataset = ds.dataset(args.dataset_root, format="parquet", partitioning="hive")

    needed_cols = [
        "pre_pt_root_id",
        "post_pt_root_id",
        "neuropil",
        "syn_count",
        "dominant_nt",
    ]

    pre_ids: set[int] = set()
    post_ids: set[int] = set()
    neuropils: set[str] = set()

    syn_hist: Dict[int, int] = {}
    neuropil_syn_hist: Dict[str, Dict[int, int]] = {}
    neuropil_nt_syn_hist: Dict[str, Dict[str, Dict[int, int]]] = {}
    global_nt_syn_hist: Dict[str, Dict[int, int]] = {}

    total_rows = 0

    scanner = dataset.scanner(columns=needed_cols, batch_size=args.batch_rows)
    for batch in scanner.to_batches():
        if args.max_rows is not None and total_rows >= args.max_rows:
            break

        if args.max_rows is not None:
            remaining = args.max_rows - total_rows
            if remaining <= 0:
                break
            if batch.num_rows > remaining:
                batch = batch.slice(0, remaining)

        total_rows += batch.num_rows

        # Unique sets
        # Convert batch once
        t_full = pa.Table.from_batches([batch]).select(
            ["pre_pt_root_id", "post_pt_root_id", "neuropil", "syn_count"]
        )
        df_full = cast(pl.DataFrame, pl.from_arrow(t_full))

        # Unique IDs
        pre_ids.update(
            df_full.select("pre_pt_root_id")
            .drop_nulls()
            .unique()
            .to_series()
            .to_list()
        )

        post_ids.update(
            df_full.select("post_pt_root_id")
            .drop_nulls()
            .unique()
            .to_series()
            .to_list()
        )

        neuropils.update(
            df_full.select("neuropil")
            .drop_nulls()
            .unique()
            .to_series()
            .to_list()
        )

        # Global syn_count histogram
        g_syn = (
            df_full.select("syn_count")
            .drop_nulls()
            .group_by("syn_count")
            .len()
        )

        for syn_count, count in g_syn.iter_rows():
            syn = int(syn_count)
            syn_hist[syn] = syn_hist.get(syn, 0) + int(count)

        # Batch -> Polars for grouped counts
        t = pa.Table.from_batches([batch]).select(["neuropil", "syn_count", "dominant_nt"])
        df = cast(pl.DataFrame, pl.from_arrow(t))
        df = df.drop_nulls(["neuropil", "syn_count"])
        # Per-neuropil syn_count hist
        g = (
            df.select(["neuropil", "syn_count"])  # type: ignore[attr-defined]
            .group_by(["neuropil", "syn_count"])  # type: ignore[attr-defined]
            .len()
            .rename({"len": "count"})
        )
        for neuropil, syn_count, count in g.iter_rows():
            np = str(neuropil)
            syn = int(syn_count)
            neuropil_syn_hist.setdefault(np, {})
            neuropil_syn_hist[np][syn] = neuropil_syn_hist[np].get(syn, 0) + int(count)

        # Per-neuropil, per-nt syn_count hist
        df2 = df.drop_nulls(["dominant_nt"]).with_columns(
            pl.col("dominant_nt")
            .cast(pl.Utf8)
            .str.split(",")
            .alias("dominant_nt_list")
        )
        df2 = df2.explode("dominant_nt_list").with_columns(
            pl.col("dominant_nt_list").cast(pl.Utf8).str.strip_chars().alias("dominant_nt_list")
        )
        df2 = df2.drop_nulls(["dominant_nt_list"])

        g2 = (
            df2.group_by(["neuropil", "dominant_nt_list", "syn_count"])  # type: ignore[attr-defined]
            .len()
            .rename({"len": "count"})
        )
        for neuropil, nt, syn_count, count in g2.iter_rows():
            np = str(neuropil)
            nt_key = str(nt)
            syn = int(syn_count)
            neuropil_nt_syn_hist.setdefault(np, {})
            neuropil_nt_syn_hist[np].setdefault(nt_key, {})
            neuropil_nt_syn_hist[np][nt_key][syn] = neuropil_nt_syn_hist[np][nt_key].get(syn, 0) + int(count)

        g3 = (
            df2.select(["dominant_nt_list", "syn_count"])  # type: ignore[attr-defined]
            .group_by(["dominant_nt_list", "syn_count"])  # type: ignore[attr-defined]
            .len()
            .rename({"len": "count"})
        )
        for nt, syn_count, count in g3.iter_rows():
            nt_key = str(nt)
            syn = int(syn_count)
            global_nt_syn_hist.setdefault(nt_key, {})
            global_nt_syn_hist[nt_key][syn] = global_nt_syn_hist[nt_key].get(syn, 0) + int(count)

    # Build JSON payload (compatible with dashboard)
    hist_items = [
        {"syn_count": int(v), "count": int(c)} for v, c in sorted(syn_hist.items(), key=lambda x: x[0])
    ]

    by_neuropil: Dict[str, Dict[str, Any]] = {}
    for neuropil, h in sorted(neuropil_syn_hist.items(), key=lambda x: x[0]):
        h_items = [
            {"syn_count": int(v), "count": int(c)} for v, c in sorted(h.items(), key=lambda x: x[0])
        ]
        by_neuropil[neuropil] = {
            "total_pairs": int(sum(h.values())),
            "histogram": h_items,
        }

    # Attach neuropil -> by_neurotransmitter
    for neuropil, by_nt in neuropil_nt_syn_hist.items():
        if neuropil not in by_neuropil:
            by_neuropil[neuropil] = {"total_pairs": 0, "histogram": []}

        by_neurotransmitter_neuropil: Dict[str, Dict[str, Any]] = {}
        for nt_name, h in by_nt.items():
            h_items = [
                {"syn_count": int(v), "count": int(c)} for v, c in sorted(h.items(), key=lambda x: x[0])
            ]
            by_neurotransmitter_neuropil[nt_name] = {
                "total_pairs": int(sum(h.values())),
                "histogram": h_items,
            }
        by_neuropil[neuropil]["by_neurotransmitter"] = by_neurotransmitter_neuropil
    by_neurotransmitter: Dict[str, Dict[str, Any]] = {}
    for nt_name, h in sorted(global_nt_syn_hist.items(), key=lambda x: x[0]):
        h_items = [
            {"syn_count": int(v), "count": int(c)} for v, c in sorted(h.items(), key=lambda x: x[0])
        ]
        by_neurotransmitter[nt_name] = {
            "total_pairs": int(sum(h.values())),
            "histogram": h_items,
        }

    summary = {
        "input": args.dataset_root,
        "max_rows": args.max_rows,
        "total_rows": total_rows,
        "neurotransmitters": NEUROTRANSMITTERS,
        "unique": {
            "pre_pt_root_id": len(pre_ids),
            "post_pt_root_id": len(post_ids),
            "neuron_ids_union": len(pre_ids | post_ids),
            "neuropil": len(neuropils),
        },
        "syn_count": {
            "min": int(min(syn_hist.keys())) if syn_hist else None,
            "max": int(max(syn_hist.keys())) if syn_hist else None,
            "median": None,
            "quantiles": {},
            "histogram": hist_items,
        },
        "by_neuropil": by_neuropil,
        "by_neurotransmitter": by_neurotransmitter,
    }

    _ensure_dir(args.out_json)
    with open(args.out_json, "w", encoding="utf-8") as f:
        json.dump(summary, f, ensure_ascii=False)

    print("Wrote Parquet-based summary JSON:", args.out_json)
    print("Total rows scanned:", total_rows)


if __name__ == "__main__":
    main()
