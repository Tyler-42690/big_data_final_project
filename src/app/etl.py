# app/etl.py

from typing import Iterable, Dict, Any, cast
import math
import logging

import pyarrow.feather as feather
from neo4j import Driver
import polars as pl

logging.basicConfig(filename='output.log',
    filemode='a', #Append mode               
    level=logging.WARNING,         
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def load_connections_arrow(
    driver: Driver,
    feather_path: str = "data/raw/proofread_connections_783.feather",
    max_rows: int | None = 100_000,
    batch_size: int = 10_000,
    clear_graph: bool = True,
) -> None:
    """
    Load a (possibly large) subset of the proofread connections table into Neo4j.

    - Uses pyarrow.feather.read_table to read the Feather/IPC file (works with this dataset).
    - Optionally limits to the first `max_rows` rows to control memory and Neo4j load.
    - Inserts Neuron nodes and CONNECTS_TO relationships in batches.

    This avoids the current incompatibility between Polars' IPC reader and this compressed file.
    """

    print(f"Loading proofread connections from {feather_path} via pyarrow...")

    table = feather.read_table(feather_path)
    total = table.num_rows
    print(f"Total rows in file: {total}")

    if max_rows is not None:
        total_use = min(total, max_rows)
        table = table.slice(0, total_use)
        print(f"Using first {total_use} rows for this run.")
    else:
        total_use = total

    df = cast(pl.DataFrame, pl.from_arrow(table))
    print("Polars dataframe shape:", df.shape)

    expected_cols = [
        "pre_pt_root_id",
        "post_pt_root_id",
        "neuropil",
        "syn_count",
        "gaba_avg",
        "ach_avg",
        "glut_avg",
        "oct_avg",
        "ser_avg",
        "da_avg",
    ]
    missing = [c for c in expected_cols if c not in df.columns]
    if missing:
        logging.error("Missing expected columns in feather file: %s", missing)
        raise ValueError(f"Missing expected columns in feather file: {missing}")

    if clear_graph:
        with driver.session() as session:
            print("Clearing existing graph data (development mode)...")
            session.run("MATCH (n) DETACH DELETE n")

    print(f"Inserting {total_use} rows into Neo4j in batches of {batch_size}...")

    # turn into list of dicts once, then chunk
    rows = df.to_dicts()

    for start in range(0, total_use, batch_size):
        end = min(start + batch_size, total_use)
        chunk = rows[start:end]
        _insert_batch(driver, chunk)
        print(f"  Inserted rows {start}–{end}")

    print("Finished loading connections via pyarrow/polars.")


def _insert_batch(driver: Driver, rows: Iterable[Dict[str, Any]]) -> None:
    """
    Insert a batch of connection rows into Neo4j as nodes and relationships.

    For each row:
      - MERGE (:Neuron {root_id: pre_pt_root_id})
      - MERGE (:Neuron {root_id: post_pt_root_id})
      - MERGE (pre)-[:CONNECTS_TO {neuropil, ...}]->(post)

    Row duplication is possible; MERGE ensures no duplicates are created.
    """
    cypher = """
    UNWIND $rows AS row
    MERGE (pre:Neuron {root_id: row.pre_pt_root_id})
    MERGE (post:Neuron {root_id: row.post_pt_root_id})
    MERGE (pre)-[c:CONNECTS_TO {neuropil: row.neuropil}]->(post)
    SET
        c.syn_count = row.syn_count,
        c.gaba_avg = row.gaba_avg,
        c.ach_avg  = row.ach_avg,
        c.glut_avg = row.glut_avg,
        c.oct_avg  = row.oct_avg,
        c.ser_avg  = row.ser_avg,
        c.da_avg   = row.da_avg
    """

    cleaned_rows = []
    for r in rows:
        cleaned = {k: (None if _is_nan(v) else v) for k, v in r.items()}
        cleaned_rows.append(cleaned)

    with driver.session() as session:
        session.run(cypher, rows=cleaned_rows)


def _is_nan(x: Any) -> bool:
    """Return True if x is a float NaN and log when detected."""
    is_nan = isinstance(x, float) and math.isnan(x)
    if is_nan:
        logging.warning("NaN detected: %r", x)
        return is_nan
    else: 
        return False

