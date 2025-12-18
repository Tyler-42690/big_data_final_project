# app/api.py
from pathlib import Path
import os
from typing import List, Dict, Any, Optional
from enum import Enum
from fastapi import FastAPI, HTTPException, Query
from fastapi.staticfiles import StaticFiles
import pyarrow.dataset as ds

from neo4j.graph import Path as Neo4jPath

from .db import get_driver
from .graph_query import (
    get_presynaptic_partners,
    get_postsynaptic_partners,
    get_two_hop_upstream,
    get_two_hop_downstream,
    get_k_hop_upstream,
    get_k_hop_downstream,
    get_k_hop_circuit
)

from .schemas import (
    CircuitResponse,
    DatasetPairResponse,
    HealthResponse,
    PartnerResponse,
    PathResponse,
    TwoHopDownstreamResponse,
    TwoHopUpstreamResponse,
)

from .dashboard import router as dashboard_router

app = FastAPI(title="FlyWire Connectome API")

# We create a single global driver for the app lifetime.
driver = get_driver()


_STATIC_DIR = Path(__file__).resolve().parent / "static"
app.mount("/static", StaticFiles(directory=str(_STATIC_DIR)), name="static")

app.include_router(dashboard_router)


_PAIRS_PARQUET_ROOT = os.getenv(
    "FLYWIRE_PAIRS_PARQUET",
    "data/aggregates/connections_with_dominant_nt_by_neuropil",
)

_PAIRS_SOURCE = os.getenv("FLYWIRE_PAIRS_SOURCE", "auto").strip().lower()


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


def _path_to_dict(p: Neo4jPath) -> Dict[str, Any]:
    """Convert a Neo4j Path to a JSON-serializable dict."""

    nodes = list(p.nodes)
    rels = list(p.relationships)

    node_ids: List[Optional[int]] = []
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


@app.get("/health", response_model=HealthResponse)
def health_check() -> Dict[str, str]:
    """Simple health check."""
    return {"status": "ok"}


@app.get("/dataset/pairs", response_model=List[DatasetPairResponse])
def dataset_pairs(
    threshold: int = Query(1, ge=0),
    neurotransmitter: Optional[str] = Query(None),
    neurotransmitter_choice: Optional[NeurotransmitterChoice] = Query(None),
    neuropil: Optional[str] = Query(None),
    neuropil_choice: Optional[NeuropilChoice] = Query(None),
    limit: int = Query(200, ge=1, le=1000),
) -> List[Dict[str, Any]]:
    """Return neuron-neuron pairs matching the current dashboard filters.

    Data source is Neo4j CONNECTS_TO relationships (must be loaded via ETL).
    """

    resolved_neurotransmitter, resolved_neuropil = _resolve_filters(
        neurotransmitter,
        neurotransmitter_choice,
        neuropil,
        neuropil_choice,
    )

    # Data source selection:
    # - neo4j: always query Neo4j
    # - parquet: parquet only (when neuropil is selected)
    # - auto (default): parquet-first (when neuropil is selected), else Neo4j
    if _PAIRS_SOURCE not in {"neo4j", "db"} and resolved_neuropil:
        partition_dir = os.path.join(_PAIRS_PARQUET_ROOT, f"neuropil={resolved_neuropil}")
        if _PAIRS_SOURCE in {"parquet", "file"} or os.path.isdir(partition_dir):
            out: List[Dict[str, Any]] = []
            nt_filter = resolved_neurotransmitter.lower() if resolved_neurotransmitter else None

            dataset = ds.dataset(partition_dir, format="parquet")
            filt = ds.field("syn_count") >= threshold
            columns = [
                "pre_pt_root_id",
                "post_pt_root_id",
                "syn_count",
                "dominant_nt",
            ]
            for batch in dataset.to_batches(columns=columns, filter=filt):
                pre = batch.column(batch.schema.get_field_index("pre_pt_root_id")).to_pylist()
                post = batch.column(batch.schema.get_field_index("post_pt_root_id")).to_pylist()
                syn = batch.column(batch.schema.get_field_index("syn_count")).to_pylist()
                dom = batch.column(batch.schema.get_field_index("dominant_nt")).to_pylist()

                for pre_id, post_id, syn_count, dominant_nt in zip(pre, post, syn, dom, strict=True):
                    if pre_id is None or post_id is None or syn_count is None:
                        continue
                    if nt_filter:
                        s = (dominant_nt or "").lower()
                        if nt_filter not in s:
                            continue
                    out.append(
                        {
                            "pre_id": int(pre_id),
                            "post_id": int(post_id),
                            "syn_count": int(syn_count),
                            "dominant_nt": dominant_nt,
                            "neuropil": resolved_neuropil,
                        }
                    )
                    if len(out) >= limit:
                        return out
            return out

    cypher = """
    MATCH (pre:Neuron)-[r:CONNECTS_TO]->(post:Neuron)
    WHERE r.syn_count >= $threshold
        AND (
            $neuropil IS NULL OR toLower(coalesce(r.neuropil, '')) = toLower($neuropil)
        )
        AND (
            $neurotransmitter IS NULL
            OR toLower(coalesce(r.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
        )
    RETURN
        pre.root_id AS pre_id,
        post.root_id AS post_id,
        r.syn_count AS syn_count,
        r.dominant_nt AS dominant_nt,
        r.neuropil AS neuropil
    ORDER BY syn_count DESC
    LIMIT $limit
    """

    with driver.session() as session:
        result = session.run(
            cypher,
            threshold=threshold,
            neuropil=resolved_neuropil,
            neurotransmitter=resolved_neurotransmitter,
            limit=limit,
        )
        return [record.data() for record in result]


@app.get("/neuron/{root_id}/presynaptic", response_model=List[PartnerResponse])
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


@app.get("/neuron/{root_id}/postsynaptic", response_model=List[PartnerResponse])
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


@app.get("/neuron/{root_id}/two_hop_upstream", response_model=List[TwoHopUpstreamResponse])
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


@app.get("/neuron/{root_id}/two_hop_downstream", response_model=List[TwoHopDownstreamResponse])
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


@app.get("/neuron/{root_id}/circuit", response_model=CircuitResponse)
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


@app.get("/neuron/{root_id}/k_hop_upstream", response_model=List[PathResponse])
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


@app.get("/neuron/{root_id}/k_hop_downstream", response_model=List[PathResponse])
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
