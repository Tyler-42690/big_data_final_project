from __future__ import annotations

from typing import List, Optional

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
