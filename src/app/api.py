# app/api.py
from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any, Optional
from enum import Enum

from neo4j.graph import Path

from .db import get_driver
from .graph_query import (
    get_presynaptic_partners,
    get_postsynaptic_partners,
    get_two_hop_upstream,
    get_two_hop_downstream,
    get_k_hop_upstream,
    get_k_hop_downstream,
)

app = FastAPI(title="FlyWire Connectome API")

# We create a single global driver for the app lifetime.
driver = get_driver()


class NeurotransmitterChoice(str, Enum):
    gaba = "GABA"
    acetylcholine = "Acetylcholine"
    glutamate = "Glutamate"
    octopamine = "Octopamine"
    serotonin = "Serotonin"
    dopamine = "Dopamine"


class NeuropilChoice(str, Enum):
    AL_L = "AL_L"
    AL_R = "AL_R"
    AME_L = "AME_L"
    AME_R = "AME_R"
    AMMC_L = "AMMC_L"
    AMMC_R = "AMMC_R"
    AOTU_L = "AOTU_L"
    AOTU_R = "AOTU_R"
    ATL_L = "ATL_L"
    ATL_R = "ATL_R"
    AVLP_L = "AVLP_L"
    AVLP_R = "AVLP_R"
    BU_L = "BU_L"
    BU_R = "BU_R"
    CAN_L = "CAN_L"
    CAN_R = "CAN_R"
    CRE_L = "CRE_L"
    CRE_R = "CRE_R"
    EB = "EB"
    EPA_L = "EPA_L"
    EPA_R = "EPA_R"
    FB = "FB"
    FLA_L = "FLA_L"
    FLA_R = "FLA_R"
    GA_L = "GA_L"
    GA_R = "GA_R"
    GNG = "GNG"
    GOR_L = "GOR_L"
    GOR_R = "GOR_R"
    IB_L = "IB_L"
    IB_R = "IB_R"
    ICL_L = "ICL_L"
    ICL_R = "ICL_R"
    IPS_L = "IPS_L"
    IPS_R = "IPS_R"
    LAL_L = "LAL_L"
    LAL_R = "LAL_R"
    LA_L = "LA_L"
    LA_R = "LA_R"
    LH_L = "LH_L"
    LH_R = "LH_R"
    LOP_L = "LOP_L"
    LOP_R = "LOP_R"
    LO_L = "LO_L"
    LO_R = "LO_R"
    MB_CA_L = "MB_CA_L"
    MB_CA_R = "MB_CA_R"
    MB_ML_L = "MB_ML_L"
    MB_ML_R = "MB_ML_R"
    MB_PED_L = "MB_PED_L"
    MB_PED_R = "MB_PED_R"
    MB_VL_L = "MB_VL_L"
    MB_VL_R = "MB_VL_R"
    ME_L = "ME_L"
    ME_R = "ME_R"
    NO = "NO"
    OCG = "OCG"
    PB = "PB"
    PLP_L = "PLP_L"
    PLP_R = "PLP_R"
    PRW = "PRW"
    PVLP_L = "PVLP_L"
    PVLP_R = "PVLP_R"
    SAD = "SAD"
    SCL_L = "SCL_L"
    SCL_R = "SCL_R"
    SIP_L = "SIP_L"
    SIP_R = "SIP_R"
    SLP_L = "SLP_L"
    SLP_R = "SLP_R"
    SMP_L = "SMP_L"
    SMP_R = "SMP_R"
    SPS_L = "SPS_L"
    SPS_R = "SPS_R"
    UNASGD = "UNASGD"
    VES_L = "VES_L"
    VES_R = "VES_R"
    WED_L = "WED_L"
    WED_R = "WED_R"


def _resolve_filters(
    neurotransmitter: Optional[str],
    neurotransmitter_choice: Optional[NeurotransmitterChoice],
    neuropil: Optional[str],
    neuropil_choice: Optional[NeuropilChoice],
) -> tuple[Optional[str], Optional[str]]:
    resolved_neurotransmitter = neurotransmitter or (
        neurotransmitter_choice.value if neurotransmitter_choice else None
    )
    resolved_neuropil = neuropil or (neuropil_choice.value if neuropil_choice else None)
    return resolved_neurotransmitter, resolved_neuropil


def _path_to_dict(p: Path) -> Dict[str, Any]:
    """Convert a Neo4j Path to a JSON-serializable dict."""

    nodes = list(p.nodes)
    rels = list(p.relationships)

    node_ids: List[int] = []
    for n in nodes:
        rid = n.get("root_id")
        if rid is None:
            rid = n.get("id")
        node_ids.append(rid)

    edges: List[Dict[str, Any]] = []
    for i, rel in enumerate(rels):
        source = node_ids[i] if i < len(node_ids) else None
        target = node_ids[i + 1] if (i + 1) < len(node_ids) else None
        edges.append(
            {
                "source_id": source,
                "target_id": target,
                "syn_count": rel.get("syn_count"),
                "neuropil": rel.get("neuropil"),
                "dominant_nt": rel.get("dominant_nt"),
                "dominant_score": rel.get("dominant_score"),
            }
        )

    return {"nodes": node_ids, "edges": edges}


@app.get("/health")
def health_check() -> Dict[str, str]:
    """Simple health check."""
    return {"status": "ok"}


@app.get("/neuron/{root_id}/presynaptic")
def api_presynaptic(
    root_id: int,
    threshold: int = Query(0, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Get direct presynaptic partners of a neuron.
    """
    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    partners = get_presynaptic_partners(
        driver,
        root_id,
        threshold=threshold,
        neurotransmitter=resolved_neurotransmitter,
        neuropil=resolved_neuropil,
    )
    if not partners:
        # Not strictly an error; you can also just return [].
        # Here we return 404 to make it explicit.
        raise HTTPException(status_code=404, detail="No presynaptic partners found")
    return partners


@app.get("/neuron/{root_id}/postsynaptic")
def api_postsynaptic(
    root_id: int,
    threshold: int = Query(0, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Get direct postsynaptic partners of a neuron.
    """
    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    partners = get_postsynaptic_partners(
        driver,
        root_id,
        threshold=threshold,
        neurotransmitter=resolved_neurotransmitter,
        neuropil=resolved_neuropil,
    )
    if not partners:
        raise HTTPException(status_code=404, detail="No postsynaptic partners found")
    return partners


@app.get("/neuron/{root_id}/two_hop_upstream")
def api_two_hop_upstream(
    root_id: int,
    threshold: int = Query(0, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Get two-hop upstream chains: pre-of-pre.
    """
    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    chains = get_two_hop_upstream(
        driver,
        root_id,
        threshold=threshold,
        neurotransmitter=resolved_neurotransmitter,
        neuropil=resolved_neuropil,
    )
    if not chains:
        raise HTTPException(status_code=404, detail="No two-hop upstream chains found")
    return chains


@app.get("/neuron/{root_id}/two_hop_downstream")
def api_two_hop_downstream(
    root_id: int,
    threshold: int = Query(0, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
) -> List[Dict[str, Any]]:
    """
    Get two-hop downstream chains: post-of-post.
    """
    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    chains = get_two_hop_downstream(
        driver,
        root_id,
        threshold=threshold,
        neurotransmitter=resolved_neurotransmitter,
        neuropil=resolved_neuropil,
    )
    if not chains:
        raise HTTPException(status_code=404, detail="No two-hop downstream chains found")
    return chains


# 如果你已经有 get_k_hop_circuit，可以再加一个：
from .graph_query import get_k_hop_circuit


@app.get("/neuron/{root_id}/circuit")
def api_circuit(
    root_id: int,
    k: int = Query(3, ge=1, le=5),
    threshold: int = Query(0, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
) -> Dict[str, Any]:
    """
    Get k-hop ego network (local circuit graph) around root_id.
    """
    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    circuit = get_k_hop_circuit(
        driver,
        root_id,
        k=k,
        threshold=threshold,
        neurotransmitter=resolved_neurotransmitter,
        neuropil=resolved_neuropil,
    )
    if not circuit.get("circuits"):
        raise HTTPException(status_code=404, detail="No circuit found for this neuron")
    return circuit


@app.get("/neuron/{root_id}/k_hop_upstream")
def api_k_hop_upstream(
    root_id: int,
    k: int = Query(2, ge=1, le=5),
    threshold: int = Query(0, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
) -> List[Dict[str, Any]]:
    """Return upstream paths up to k hops, filtered by syn_count threshold."""

    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    paths = get_k_hop_upstream(
        driver,
        root_id,
        k=k,
        threshold=threshold,
        neurotransmitter=resolved_neurotransmitter,
        neuropil=resolved_neuropil,
    )
    if not paths:
        raise HTTPException(status_code=404, detail="No upstream paths found")
    return [_path_to_dict(p) for p in paths]


@app.get("/neuron/{root_id}/k_hop_downstream")
def api_k_hop_downstream(
    root_id: int,
    k: int = Query(2, ge=1, le=5),
    threshold: int = Query(0, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
) -> List[Dict[str, Any]]:
    """Return downstream paths up to k hops, filtered by syn_count threshold."""

    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    paths = get_k_hop_downstream(
        driver,
        root_id,
        k=k,
        threshold=threshold,
        neurotransmitter=resolved_neurotransmitter,
        neuropil=resolved_neuropil,
    )
    if not paths:
        raise HTTPException(status_code=404, detail="No downstream paths found")
    return [_path_to_dict(p) for p in paths]
