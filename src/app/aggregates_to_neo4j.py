from __future__ import annotations

import argparse
import json
import os
from typing import Any, Dict, Optional

from neo4j import Driver

from .db import close_driver, get_driver


def push_summary_json_to_neo4j(
    driver: Driver,
    *,
    summary_json_path: str,
    dataset_id: str = "flywire",
    clear_existing: bool = False,
) -> None:
    """Push the precomputed dashboard summary JSON into Neo4j.

    This fulfills the "push aggregates back into the Big Data system" requirement by storing
    the gold/aggregated summary as a Neo4j node property.

    The dashboard/API can later read it back from Neo4j instead of the filesystem.
    """

    with open(summary_json_path, "r", encoding="utf-8") as f:
        summary: Dict[str, Any] = json.load(f)

    summary_json_str = json.dumps(summary, ensure_ascii=False)

    total_rows = summary.get("total_rows")
    max_rows = summary.get("max_rows")
    uniq = summary.get("unique") or {}
    syn = summary.get("syn_count") or {}

    params = {
        "id": dataset_id,
        "summary_json": summary_json_str,
        "input": summary.get("input"),
        "total_rows": int(total_rows) if isinstance(total_rows, (int, float)) else None,
        "max_rows": int(max_rows) if isinstance(max_rows, (int, float)) else None,
        "pre_pt_root_id": _safe_int(uniq.get("pre_pt_root_id")),
        "post_pt_root_id": _safe_int(uniq.get("post_pt_root_id")),
        "neuron_ids_union": _safe_int(uniq.get("neuron_ids_union")),
        "neuropil": _safe_int(uniq.get("neuropil")),
        "syn_min": _safe_int(syn.get("min")),
        "syn_max": _safe_int(syn.get("max")),
        "syn_median": _safe_int(syn.get("median")),
    }

    with driver.session() as session:
        if clear_existing:
            session.run("MATCH (s:DatasetSummary {id: $id}) DETACH DELETE s", id=dataset_id)

        # Constraint is idempotent in Neo4j 5+.
        session.run(
            "CREATE CONSTRAINT dataset_summary_id IF NOT EXISTS "
            "FOR (s:DatasetSummary) REQUIRE s.id IS UNIQUE"
        )

        session.run(
            """
            MERGE (s:DatasetSummary {id: $id})
            SET
              s.summary_json = $summary_json,
              s.input = $input,
              s.total_rows = $total_rows,
              s.max_rows = $max_rows,
              s.pre_pt_root_id = $pre_pt_root_id,
              s.post_pt_root_id = $post_pt_root_id,
              s.neuron_ids_union = $neuron_ids_union,
              s.neuropil = $neuropil,
              s.syn_min = $syn_min,
              s.syn_max = $syn_max,
              s.syn_median = $syn_median,
              s.updated_at = datetime()
            """,
            **params,
        )


def fetch_summary_json_from_neo4j(
    driver: Driver,
    *,
    dataset_id: str = "flywire",
) -> Optional[Dict[str, Any]]:
    """Fetch the stored summary JSON from Neo4j.

    Returns the parsed JSON dict, or None if missing.
    """

    with driver.session() as session:
        rec = session.run(
            "MATCH (s:DatasetSummary {id: $id}) RETURN s.summary_json AS summary_json",
            id=dataset_id,
        ).single()

        if not rec:
            return None

        raw = rec.get("summary_json")
        if not raw:
            return None

        return json.loads(raw)


def _safe_int(x: Any) -> Optional[int]:
    try:
        if x is None:
            return None
        return int(x)
    except Exception:
        return None


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Push aggregated dashboard summary JSON into Neo4j")
    p.add_argument(
        "--summary-json",
        default=os.getenv("FLYWIRE_SUMMARY_JSON", "data/summary/flywire_summary.json"),
        help="Path to the precomputed summary JSON (default: env FLYWIRE_SUMMARY_JSON or data/summary/flywire_summary.json)",
    )
    p.add_argument(
        "--dataset-id",
        default="flywire",
        help="Neo4j DatasetSummary id (default: flywire)",
    )
    p.add_argument(
        "--clear-existing",
        action="store_true",
        help="Delete existing DatasetSummary node with the same id before inserting",
    )
    return p.parse_args()


def main() -> None:
    args = _parse_args()
    driver = get_driver()
    try:
        push_summary_json_to_neo4j(
            driver,
            summary_json_path=args.summary_json,
            dataset_id=args.dataset_id,
            clear_existing=args.clear_existing,
        )
        print(
            "Pushed summary JSON to Neo4j as (:DatasetSummary {id: '%s'})." % args.dataset_id
        )
    finally:
        close_driver(driver)


if __name__ == "__main__":
    main()
