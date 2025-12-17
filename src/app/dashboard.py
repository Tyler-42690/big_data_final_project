from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

from fastapi import APIRouter, HTTPException
from fastapi.responses import FileResponse

from .schemas import SynCountSummaryResponse

router = APIRouter(tags=["dashboard"])

SUMMARY_JSON_PATH = os.getenv("FLYWIRE_SUMMARY_JSON", "data/summary/flywire_summary.json")

_STATIC_DIR = Path(__file__).resolve().parent / "static"
_DASHBOARD_DIR = _STATIC_DIR / "dashboard"


@router.get("/dataset/summary", response_model=SynCountSummaryResponse)
def dataset_summary() -> Dict[str, Any]:
    """Return a precomputed dataset summary JSON (generated offline by summarize_data.py)."""

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
