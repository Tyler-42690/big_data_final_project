from __future__ import annotations

from typing import Dict, List, Optional

from pydantic import BaseModel, ConfigDict


class HealthResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    status: str


class PartnerResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    partner_id: int
    syn_count: int
    neuropil: Optional[str] = None

    dominant_nt: Optional[str] = None
    dominant_score: Optional[float] = None

    gaba_avg: Optional[float] = None
    ach_avg: Optional[float] = None
    glut_avg: Optional[float] = None
    oct_avg: Optional[float] = None
    ser_avg: Optional[float] = None
    da_avg: Optional[float] = None


class TwoHopUpstreamResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pre2_id: int
    pre1_id: int

    syn_count_1: int
    syn_count_2: int

    neuropil_1: Optional[str] = None
    neuropil_2: Optional[str] = None


class TwoHopDownstreamResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    post1_id: int
    post2_id: int

    syn_count_1: int
    syn_count_2: int

    neuropil_1: Optional[str] = None
    neuropil_2: Optional[str] = None


class PathEdgeResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_id: Optional[int] = None
    target_id: Optional[int] = None

    syn_count: Optional[int] = None
    neuropil: Optional[str] = None

    dominant_nt: Optional[str] = None
    dominant_score: Optional[float] = None


class PathResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    nodes: List[Optional[int]]
    edges: List[PathEdgeResponse]


class CircuitConnectionResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    source_root_id: int
    target_root_id: int

    neuropil: Optional[str] = None
    syn_count: Optional[int] = None

    dominant_nt: Optional[str] = None


class CircuitResponseItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    connections: List[CircuitConnectionResponse]


class CircuitResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    circuits: List[CircuitResponseItem]


class SynCountHistogramItem(BaseModel):
    model_config = ConfigDict(extra="forbid")

    syn_count: int
    count: int


class SynCountSummaryResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    input: str
    max_rows: Optional[int] = None
    total_rows: int

    unique: dict
    syn_count: dict

    # Optional fields used by dashboards (kept optional for backward-compatible JSON summaries).
    neurotransmitters: Optional[List[str]] = None
    by_neuropil: Optional[Dict[str, "NeuropilSynCountSummary"]] = None

    # Optional global breakdown used by dashboards for filtering
    by_neurotransmitter: Optional[Dict[str, "NeuropilSynCountSummaryNT"]] = None


class NeuropilSynCountSummary(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_pairs: int
    histogram: List[SynCountHistogramItem]

    # Optional breakdown used by dashboards to color circles by dominant neurotransmitter
    # after applying a synapse-count threshold.
    by_neurotransmitter: Optional[Dict[str, "NeuropilSynCountSummaryNT"]] = None


class NeuropilSynCountSummaryNT(BaseModel):
    model_config = ConfigDict(extra="forbid")

    total_pairs: int
    histogram: List[SynCountHistogramItem]


class DatasetPairResponse(BaseModel):
    model_config = ConfigDict(extra="forbid")

    pre_id: int
    post_id: int
    syn_count: int

    dominant_nt: Optional[str] = None
    neuropil: Optional[str] = None


# Ensure forward refs are resolved in Pydantic v2 (prevents extra_forbidden on nested fields)
SynCountSummaryResponse.model_rebuild()
NeuropilSynCountSummary.model_rebuild()
