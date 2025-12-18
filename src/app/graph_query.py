# app/graph_query.py

from typing import List, Dict, Any, Optional
from neo4j import Driver
from neo4j.graph import Path


def get_postsynaptic_partners(
    driver: Driver,
    root_id: int,
    threshold: int = 0,
    neurotransmitter: Optional[str] = None,
    neuropil: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return all direct postsynaptic partners of the given neuron.

    Direction: (n) -[:CONNECTS_TO]-> (m)
    """
    cypher = """
    MATCH (n:Neuron {root_id: $root_id})-[r:CONNECTS_TO]->(m:Neuron)
        WHERE r.syn_count >= $threshold
            AND (
                $neuropil IS NULL OR toLower(coalesce(r.neuropil, '')) = toLower($neuropil)
            )
            AND (
                $neurotransmitter IS NULL
                OR toLower(coalesce(r.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
            )
    RETURN
        m.root_id AS partner_id,
        r.syn_count AS syn_count,
        r.neuropil AS neuropil,
        r.dominant_nt AS dominant_nt,
        r.dominant_score AS dominant_score,
        r.gaba_avg AS gaba_avg,
        r.ach_avg  AS ach_avg,
        r.glut_avg AS glut_avg,
        r.oct_avg  AS oct_avg,
        r.ser_avg  AS ser_avg,
        r.da_avg   AS da_avg
    ORDER BY syn_count DESC
    """
    with driver.session() as session:
        result = session.run(
            cypher,
            root_id=root_id,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )
        return [record.data() for record in result]


def get_presynaptic_partners(
    driver: Driver,
    root_id: int,
    threshold: int = 0,
    neurotransmitter: Optional[str] = None,
    neuropil: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return all direct presynaptic partners of the given neuron.

    Direction: (m) -[:CONNECTS_TO]-> (n)
    """
    cypher = """
    MATCH (m:Neuron)-[r:CONNECTS_TO]->(n:Neuron {root_id: $root_id})
        WHERE r.syn_count >= $threshold
            AND (
                $neuropil IS NULL OR toLower(coalesce(r.neuropil, '')) = toLower($neuropil)
            )
            AND (
                $neurotransmitter IS NULL
                OR toLower(coalesce(r.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
            )
    RETURN
        m.root_id AS partner_id,
        r.syn_count AS syn_count,
        r.neuropil AS neuropil,
        r.dominant_nt AS dominant_nt,
        r.dominant_score AS dominant_score,
        r.gaba_avg AS gaba_avg,
        r.ach_avg  AS ach_avg,
        r.glut_avg AS glut_avg,
        r.oct_avg  AS oct_avg,
        r.ser_avg  AS ser_avg,
        r.da_avg   AS da_avg
    ORDER BY syn_count DESC
    """
    with driver.session() as session:
        result = session.run(
            cypher,
            root_id=root_id,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )
        return [record.data() for record in result]


def get_two_hop_upstream(
    driver: Driver,
    root_id: int,
    threshold: int = 0,
    neurotransmitter: Optional[str] = None,
    neuropil: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return 2-hop upstream chain: pre-of-pre.

    Pattern: (pre2) -> (pre1) -> (target)
    """
    cypher = """
    MATCH (pre2:Neuron)-[r2:CONNECTS_TO]->(pre1:Neuron)-[r1:CONNECTS_TO]->(target:Neuron {root_id: $root_id})
    WHERE r1.syn_count >= $threshold AND r2.syn_count >= $threshold
            AND (
                $neuropil IS NULL
                OR (
                    toLower(coalesce(r1.neuropil, '')) = toLower($neuropil)
                    AND toLower(coalesce(r2.neuropil, '')) = toLower($neuropil)
                )
            )
            AND (
                $neurotransmitter IS NULL
                OR (
                    toLower(coalesce(r1.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
                    AND toLower(coalesce(r2.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
                )
            )
    RETURN
        pre2.root_id AS pre2_id,
        pre1.root_id AS pre1_id,
        r1.syn_count AS syn_count_1,
        r2.syn_count AS syn_count_2,
        r1.neuropil AS neuropil_1,
        r2.neuropil AS neuropil_2
    """
    with driver.session() as session:
        result = session.run(
            cypher,
            root_id=root_id,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )
        return [record.data() for record in result]


def get_two_hop_downstream(
    driver: Driver,
    root_id: int,
    threshold: int = 0,
    neurotransmitter: Optional[str] = None,
    neuropil: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    Return 2-hop downstream chain: post-of-post.

    Pattern: (target) -> (post1) -> (post2)
    """
    cypher = """
    MATCH (target:Neuron {root_id: $root_id})-[r1:CONNECTS_TO]->(post1:Neuron)-[r2:CONNECTS_TO]->(post2:Neuron)
    WHERE r1.syn_count >= $threshold AND r2.syn_count >= $threshold
            AND (
                $neuropil IS NULL
                OR (
                    toLower(coalesce(r1.neuropil, '')) = toLower($neuropil)
                    AND toLower(coalesce(r2.neuropil, '')) = toLower($neuropil)
                )
            )
            AND (
                $neurotransmitter IS NULL
                OR (
                    toLower(coalesce(r1.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
                    AND toLower(coalesce(r2.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
                )
            )
    RETURN
        post1.root_id AS post1_id,
        post2.root_id AS post2_id,
        r1.syn_count AS syn_count_1,
        r2.syn_count AS syn_count_2,
        r1.neuropil AS neuropil_1,
        r2.neuropil AS neuropil_2
    """
    with driver.session() as session:
        result = session.run(
            cypher,
            root_id=root_id,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )
        return [record.data() for record in result]


def get_k_hop_downstream(
    driver: Driver,
    root_id: int,
    k: int,
    threshold: int = 0,
    neurotransmitter: Optional[str] = None,
    neuropil: Optional[str] = None,
) -> List[Path]:
    """
    Return all nodes reachable within k hops downstream.

    Pattern: (start) -[:CONNECTS_TO*1..k]-> (other)
    """
    cypher = """
    MATCH p = (start:Neuron {root_id: $root_id})-[r:CONNECTS_TO*1..$k]->(other:Neuron)
    WHERE ALL(rel IN r WHERE rel.syn_count >= $threshold)
            AND (
                $neuropil IS NULL OR ALL(rel IN r WHERE toLower(coalesce(rel.neuropil, '')) = toLower($neuropil))
            )
            AND (
                $neurotransmitter IS NULL
                OR ALL(rel IN r WHERE toLower(coalesce(rel.dominant_nt, '')) CONTAINS toLower($neurotransmitter))
            )
    RETURN p
    """
    k_int = int(k)
    if k_int < 1:
        raise ValueError("k must be >= 1")

    # NOTE: Neo4j does not allow parameterized variable-length patterns like *1..$k.
    # We embed the validated integer directly.
    cypher = f"""
    MATCH p = (start:Neuron {{root_id: $root_id}})-[r:CONNECTS_TO*1..{k_int}]->(other:Neuron)
    WHERE ALL(rel IN r WHERE rel.syn_count >= $threshold)
            AND (
                $neuropil IS NULL OR ALL(rel IN r WHERE toLower(coalesce(rel.neuropil, '')) = toLower($neuropil))
            )
            AND (
                $neurotransmitter IS NULL
                OR ALL(rel IN r WHERE toLower(coalesce(rel.dominant_nt, '')) CONTAINS toLower($neurotransmitter))
            )
    RETURN p
    """
    with driver.session() as session:
        result = session.run(
            cypher,
            root_id=root_id,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )
        # Here we just return the raw paths; later you can format them as needed
        return [record["p"] for record in result]


def get_k_hop_upstream(
    driver: Driver,
    root_id: int,
    k: int,
    threshold: int = 0,
    neurotransmitter: Optional[str] = None,
    neuropil: Optional[str] = None,
) -> List[Path]:
    """
    Return all nodes reachable within k hops upstream.

    Pattern: (other) -[:CONNECTS_TO*1..k]-> (start)
    """
    cypher = """
    MATCH p = (other:Neuron)-[r:CONNECTS_TO*1..$k]->(start:Neuron {root_id: $root_id})
    WHERE ALL(rel IN r WHERE rel.syn_count >= $threshold)
            AND (
                $neuropil IS NULL OR ALL(rel IN r WHERE toLower(coalesce(rel.neuropil, '')) = toLower($neuropil))
            )
            AND (
                $neurotransmitter IS NULL
                OR ALL(rel IN r WHERE toLower(coalesce(rel.dominant_nt, '')) CONTAINS toLower($neurotransmitter))
            )
    RETURN p
    """
    k_int = int(k)
    if k_int < 1:
        raise ValueError("k must be >= 1")

    # NOTE: Neo4j does not allow parameterized variable-length patterns like *1..$k.
    # We embed the validated integer directly.
    cypher = f"""
    MATCH p = (other:Neuron)-[r:CONNECTS_TO*1..{k_int}]->(start:Neuron {{root_id: $root_id}})
    WHERE ALL(rel IN r WHERE rel.syn_count >= $threshold)
            AND (
                $neuropil IS NULL OR ALL(rel IN r WHERE toLower(coalesce(rel.neuropil, '')) = toLower($neuropil))
            )
            AND (
                $neurotransmitter IS NULL
                OR ALL(rel IN r WHERE toLower(coalesce(rel.dominant_nt, '')) CONTAINS toLower($neurotransmitter))
            )
    RETURN p
    """
    with driver.session() as session:
        result = session.run(
            cypher,
            root_id=root_id,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )
        return [record["p"] for record in result]
    


def get_k_hop_circuit(
    driver: Driver,
    root_id: int,
    k: int = 3,
    threshold: int = 0,
    neurotransmitter: Optional[str] = None,
    neuropil: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Return the k-hop ego network (local circuit) around `root_id`.

    Output format:
    - A list of circuits (currently one circuit per query)
    - Each circuit contains a list of connections
    - Each connection contains source/target root_id, neuropil, syn_count, dominant_nt
    """

    # Safety guard: don't allow arbitrary k from outside
    if k < 1 or k > 5:
        raise ValueError("k must be between 1 and 5")

    # We have to interpolate k into the Cypher string, because Neo4j
    # does not allow parameters in the variable-length pattern `*1..$k`.
    node_query = f"""
    MATCH p = (center:Neuron {{root_id: $root_id}})-[r:CONNECTS_TO*1..{k}]-(n:Neuron)
        WHERE ALL(rel IN r WHERE rel.syn_count >= $threshold)
            AND (
                $neuropil IS NULL OR ALL(rel IN r WHERE toLower(coalesce(rel.neuropil, '')) = toLower($neuropil))
            )
            AND (
                $neurotransmitter IS NULL
                OR ALL(rel IN r WHERE toLower(coalesce(rel.dominant_nt, '')) CONTAINS toLower($neurotransmitter))
            )
    RETURN DISTINCT n.root_id AS id
    """

    with driver.session() as session:
        node_result = session.run(
            node_query,
            root_id=root_id,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )
        node_ids = [record["id"] for record in node_result]

        if not node_ids:
            return {"circuits": []}

        if root_id not in node_ids:
            node_ids.append(root_id)

        edge_result = session.run(
            """
            MATCH (a:Neuron)-[r:CONNECTS_TO]->(b:Neuron)
            WHERE a.root_id IN $ids AND b.root_id IN $ids AND r.syn_count >= $threshold
                            AND (
                                $neuropil IS NULL OR toLower(coalesce(r.neuropil, '')) = toLower($neuropil)
                            )
                            AND (
                                $neurotransmitter IS NULL
                                OR toLower(coalesce(r.dominant_nt, '')) CONTAINS toLower($neurotransmitter)
                            )
            RETURN
                a.root_id AS source_id,
                b.root_id AS target_id,
                r.neuropil AS neuropil,
                r.syn_count AS syn_count,
                r.dominant_nt AS dominant_nt
            """,
            ids=node_ids,
            threshold=threshold,
            neurotransmitter=neurotransmitter,
            neuropil=neuropil,
        )

        connections: List[Dict[str, Any]] = []
        for record in edge_result:
            connections.append(
                {
                    "source_root_id": record.get("source_id"),
                    "target_root_id": record.get("target_id"),
                    "neuropil": record.get("neuropil"),
                    "syn_count": record.get("syn_count"),
                    "dominant_nt": record.get("dominant_nt"),
                }
            )

    return {"circuits": [{"connections": connections}]}
