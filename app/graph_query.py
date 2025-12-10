# app/graph_query.py

from typing import List, Dict, Any
from neo4j import Driver


def get_postsynaptic_partners(driver: Driver, root_id: int) -> List[Dict[str, Any]]:
    """
    Return all direct postsynaptic partners of the given neuron.

    Direction: (n) -[:CONNECTS_TO]-> (m)
    """
    cypher = """
    MATCH (n:Neuron {root_id: $root_id})-[r:CONNECTS_TO]->(m:Neuron)
    RETURN
        m.root_id AS partner_id,
        r.syn_count AS syn_count,
        r.neuropil AS neuropil,
        r.gaba_avg AS gaba_avg,
        r.ach_avg  AS ach_avg,
        r.glut_avg AS glut_avg,
        r.oct_avg  AS oct_avg,
        r.ser_avg  AS ser_avg,
        r.da_avg   AS da_avg
    ORDER BY syn_count DESC
    """
    with driver.session() as session:
        result = session.run(cypher, root_id=root_id)
        return [record.data() for record in result]


def get_presynaptic_partners(driver: Driver, root_id: int) -> List[Dict[str, Any]]:
    """
    Return all direct presynaptic partners of the given neuron.

    Direction: (m) -[:CONNECTS_TO]-> (n)
    """
    cypher = """
    MATCH (m:Neuron)-[r:CONNECTS_TO]->(n:Neuron {root_id: $root_id})
    RETURN
        m.root_id AS partner_id,
        r.syn_count AS syn_count,
        r.neuropil AS neuropil,
        r.gaba_avg AS gaba_avg,
        r.ach_avg  AS ach_avg,
        r.glut_avg AS glut_avg,
        r.oct_avg  AS oct_avg,
        r.ser_avg  AS ser_avg,
        r.da_avg   AS da_avg
    ORDER BY syn_count DESC
    """
    with driver.session() as session:
        result = session.run(cypher, root_id=root_id)
        return [record.data() for record in result]


def get_two_hop_upstream(driver: Driver, root_id: int):
    """
    Return 2-hop upstream chain: pre-of-pre.

    Pattern: (pre2) -> (pre1) -> (target)
    """
    cypher = """
    MATCH (pre2:Neuron)-[r2:CONNECTS_TO]->(pre1:Neuron)-[r1:CONNECTS_TO]->(target:Neuron {root_id: $root_id})
    RETURN
        pre2.root_id AS pre2_id,
        pre1.root_id AS pre1_id,
        r1.syn_count AS syn_count_1,
        r2.syn_count AS syn_count_2
    """
    with driver.session() as session:
        result = session.run(cypher, root_id=root_id)
        return [record.data() for record in result]


def get_two_hop_downstream(driver: Driver, root_id: int):
    """
    Return 2-hop downstream chain: post-of-post.

    Pattern: (target) -> (post1) -> (post2)
    """
    cypher = """
    MATCH (target:Neuron {root_id: $root_id})-[r1:CONNECTS_TO]->(post1:Neuron)-[r2:CONNECTS_TO]->(post2:Neuron)
    RETURN
        post1.root_id AS post1_id,
        post2.root_id AS post2_id,
        r1.syn_count AS syn_count_1,
        r2.syn_count AS syn_count_2
    """
    with driver.session() as session:
        result = session.run(cypher, root_id=root_id)
        return [record.data() for record in result]


def get_k_hop_downstream(driver: Driver, root_id: int, k: int):
    """
    Return all nodes reachable within k hops downstream.

    Pattern: (start) -[:CONNECTS_TO*1..k]-> (other)
    """
    cypher = """
    MATCH p = (start:Neuron {root_id: $root_id})-[r:CONNECTS_TO*1..$k]->(other:Neuron)
    RETURN p
    """
    with driver.session() as session:
        result = session.run(cypher, root_id=root_id, k=k)
        # Here we just return the raw paths; later you can format them as needed
        return [record["p"] for record in result]


def get_k_hop_upstream(driver: Driver, root_id: int, k: int):
    """
    Return all nodes reachable within k hops upstream.

    Pattern: (other) -[:CONNECTS_TO*1..k]-> (start)
    """
    cypher = """
    MATCH p = (other:Neuron)-[r:CONNECTS_TO*1..$k]->(start:Neuron {root_id: $root_id})
    RETURN p
    """
    with driver.session() as session:
        result = session.run(cypher, root_id=root_id, k=k)
        return [record["p"] for record in result]
    


def get_k_hop_circuit(driver: Driver, root_id: int, k: int = 3) -> Dict[str, List[Dict[str, Any]]]:
    """
    Return the k-hop ego network (local circuit graph) around `root_id`.

    - Nodes: all Neuron nodes reachable within k hops (incoming or outgoing).
    - Edges: all CONNECTS_TO relationships among those nodes.
    """

    # Safety guard: don't allow arbitrary k from outside
    if k < 1 or k > 5:
        raise ValueError("k must be between 1 and 5")

    # We have to interpolate k into the Cypher string, because Neo4j
    # does not allow parameters in the variable-length pattern `*1..$k`.
    node_query = f"""
    MATCH (center:Neuron {{root_id: $root_id}})
    MATCH (center)-[*1..{k}]-(n:Neuron)
    RETURN DISTINCT n.root_id AS id
    """

    with driver.session() as session:
        node_result = session.run(node_query, root_id=root_id)
        node_ids = [record["id"] for record in node_result]

        if not node_ids:
            return {"nodes": [], "edges": []}

        edge_result = session.run(
            """
            MATCH (a:Neuron)-[r:CONNECTS_TO]->(b:Neuron)
            WHERE a.root_id IN $ids AND b.root_id IN $ids
            RETURN
                a.root_id AS source_id,
                b.root_id AS target_id,
                r.neuropil AS neuropil,
                r.syn_count AS syn_count,
                r.gaba_avg AS gaba_avg,
                r.ach_avg AS ach_avg,
                r.glut_avg AS glut_avg,
                r.oct_avg AS oct_avg,
                r.ser_avg AS ser_avg,
                r.da_avg AS da_avg
            """,
            ids=node_ids,
        )

        edges = [dict(record) for record in edge_result]

    nodes = [{"id": nid} for nid in node_ids]
    return {"nodes": nodes, "edges": edges}
