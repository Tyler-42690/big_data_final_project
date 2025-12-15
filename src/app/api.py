# app/api.py
from fastapi import FastAPI, HTTPException, Query
from typing import List, Dict, Any

from app.db import get_driver
from app.graph_query import (
    get_presynaptic_partners,
    get_postsynaptic_partners,
    get_two_hop_upstream,
    get_two_hop_downstream,
)

app = FastAPI(title="FlyWire Connectome API")

# We create a single global driver for the app lifetime.
driver = get_driver()


@app.get("/health")
def health_check() -> Dict[str, str]:
    """Simple health check."""
    return {"status": "ok"}


@app.get("/neuron/{root_id}/presynaptic")
def api_presynaptic(root_id: int) -> List[Dict[str, Any]]:
    """
    Get direct presynaptic partners of a neuron.
    """
    partners = get_presynaptic_partners(driver, root_id)
    if not partners:
        # Not strictly an error; you can also just return [].
        # Here we return 404 to make it explicit.
        raise HTTPException(status_code=404, detail="No presynaptic partners found")
    return partners


@app.get("/neuron/{root_id}/postsynaptic")
def api_postsynaptic(root_id: int) -> List[Dict[str, Any]]:
    """
    Get direct postsynaptic partners of a neuron.
    """
    partners = get_postsynaptic_partners(driver, root_id)
    if not partners:
        raise HTTPException(status_code=404, detail="No postsynaptic partners found")
    return partners


@app.get("/neuron/{root_id}/two_hop_upstream")
def api_two_hop_upstream(root_id: int) -> List[Dict[str, Any]]:
    """
    Get two-hop upstream chains: pre-of-pre.
    """
    chains = get_two_hop_upstream(driver, root_id)
    if not chains:
        raise HTTPException(status_code=404, detail="No two-hop upstream chains found")
    return chains


@app.get("/neuron/{root_id}/two_hop_downstream")
def api_two_hop_downstream(root_id: int) -> List[Dict[str, Any]]:
    """
    Get two-hop downstream chains: post-of-post.
    """
    chains = get_two_hop_downstream(driver, root_id)
    if not chains:
        raise HTTPException(status_code=404, detail="No two-hop downstream chains found")
    return chains


# 如果你已经有 get_k_hop_circuit，可以再加一个：
from app.graph_query import get_k_hop_circuit


@app.get("/neuron/{root_id}/circuit")
def api_circuit(
    root_id: int,
    k: int = Query(3, ge=1, le=5),
) -> Dict[str, Any]:
    """
    Get k-hop ego network (local circuit graph) around root_id.
    """
    circuit = get_k_hop_circuit(driver, root_id, k=k)
    if not circuit["nodes"]:
        raise HTTPException(status_code=404, detail="No circuit found for this neuron")
    return circuit
