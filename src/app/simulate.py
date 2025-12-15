# app/simulate.py

from typing import List, Dict, Any
from neo4j import Driver


def get_outgoing_edges(driver: Driver, root_id: int) -> List[Dict[str, Any]]:
    """
    Fetch all outgoing edges from the given neuron.
    """
    cypher = """
    MATCH (n:Neuron {root_id: $root_id})-[r:CONNECTS_TO]->(m:Neuron)
    RETURN
        n.root_id AS source_id,
        m.root_id AS target_id,
        r.syn_count AS syn_count,
        r.neuropil AS neuropil
    """
    with driver.session() as session:
        result = session.run(cypher, root_id=root_id)
        return [record.data() for record in result]


def simulate_silence(driver: Driver, root_id: int) -> List[Dict[str, Any]]:
    """
    Simulate silencing a neuron:
    - For conceptual analysis, we treat all its outgoing synapses as having effective weight 0.
    - We do NOT modify the database; we only compute a "what-if" view.
    """
    edges = get_outgoing_edges(driver, root_id)
    simulated = []
    for e in edges:
        simulated.append(
            {
                "source_id": e["source_id"],
                "target_id": e["target_id"],
                "original_syn_count": e["syn_count"],
                "effective_syn_count": 0,
                "neuropil": e["neuropil"],
            }
        )
    return simulated


def simulate_boost(driver: Driver, root_id: int, factor: float = 2.0) -> List[Dict[str, Any]]:
    """
    Simulate boosting a neuron:
    - Multiply syn_count of all outgoing edges by `factor`.
    - Again, this is a read-only simulation; we do NOT write back to Neo4j.
    """
    edges = get_outgoing_edges(driver, root_id)
    simulated = []
    for e in edges:
        new_weight = (e["syn_count"] or 0) * factor
        simulated.append(
            {
                "source_id": e["source_id"],
                "target_id": e["target_id"],
                "original_syn_count": e["syn_count"],
                "effective_syn_count": new_weight,
                "neuropil": e["neuropil"],
            }
        )
    return simulated
