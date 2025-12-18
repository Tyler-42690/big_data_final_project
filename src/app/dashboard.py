from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from .schemas import SynCountSummaryResponse
from .db import get_driver, close_driver
from .aggregates_to_neo4j import fetch_summary_json_from_neo4j

router = APIRouter(tags=["dashboard"])

SUMMARY_JSON_PATH = os.getenv("FLYWIRE_SUMMARY_JSON", "data/summary/flywire_summary.json")
SUMMARY_SOURCE = os.getenv("FLYWIRE_SUMMARY_SOURCE", "file").strip().lower()
SUMMARY_DATASET_ID = os.getenv("FLYWIRE_SUMMARY_DATASET_ID", "flywire").strip() or "flywire"

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_DASHBOARD_DIR = _STATIC_DIR / "dashboard"


@router.get("/dataset/summary", response_model=SynCountSummaryResponse)
def dataset_summary() -> Dict[str, Any]:
    """Return a precomputed dataset summary JSON (generated offline by summarize_data.py)."""

    # Optional: serve summary from Neo4j (to satisfy "push aggregates back to Big Data system").
    if SUMMARY_SOURCE in {"neo4j", "db"}:
        driver = get_driver()
        try:
            data = fetch_summary_json_from_neo4j(driver, dataset_id=SUMMARY_DATASET_ID)
        finally:
            close_driver(driver)

        if data is None:
            raise HTTPException(
                status_code=404,
                detail=(
                    "Summary not found in Neo4j. Push it with: "
                    "python -m app.aggregates_to_neo4j --summary-json "
                    f"{SUMMARY_JSON_PATH} --dataset-id {SUMMARY_DATASET_ID}"
                ),
            )
        return data

    if not os.path.exists(SUMMARY_JSON_PATH):
        raise HTTPException(
            status_code=404,
            detail=(
                f"Summary file not found at {SUMMARY_JSON_PATH}. "
                "Generate it with: python summarize_data.py --input data/raw/proofread_connections_783.feather "
                f"--out-json {SUMMARY_JSON_PATH}"
            ),
        )

    with open(SUMMARY_JSON_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


@router.get("/dashboard/syn_count")
def syn_count_dashboard() -> FileResponse:
    """Interactive syn_count histogram dashboard (static HTML + JS)."""

    html_path = _DASHBOARD_DIR / "syn_count.html"
    if not html_path.exists():
        raise HTTPException(status_code=500, detail=f"Missing dashboard file: {html_path}")

    return FileResponse(str(html_path), media_type="text/html")
