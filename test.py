"""CI tests for Neo4j connectivity, ETL, and dashboard data provenance.

These tests are intended to run in docker-compose CI where Neo4j is available.
"""

import json
import os
from typing import Generator
import pathlib as pathLib
import pytest
from neo4j import Driver

from src.app.aggregates_to_neo4j import push_summary_json_to_neo4j
from src.app.db import check_connection, close_driver, get_driver
from src.app.etl import load_connections_arrow

# 1. FIXTURE: Handles Setup and Teardown automatically
@pytest.fixture(scope="module")
def neo4j_driver() -> Generator[Driver, None, None]:
    """
    Creates a Neo4j driver instance for the test module.
    Closes the driver automatically after all tests in this file finish.
    """
    driver_instance = get_driver()
    yield driver_instance
    close_driver(driver_instance)

# FIX: Argument name changed to 'neo4j_driver' to match the fixture above
def test_db_connection(neo4j_driver: Driver) -> None:
    """
    Tests that the application can successfully connect to Neo4j.
    """
    # If test_connection raises an error, pytest will mark this as Failed.
    try:
        check_connection(neo4j_driver)
    except Exception as e:
        pytest.fail(f"Database connection failed: {e}")

    # Additional assertion: Check verify_connectivity explicitly
    try:
        neo4j_driver.verify_connectivity()
    except Exception as e:
        pytest.fail(f"Driver could not verify connectivity: {e}")

# FIX: Argument name changed to 'neo4j_driver'
def test_load_connections(neo4j_driver: Driver) -> None:
    """
    Tests the ETL process by loading a subset of data and asserting
    that the data exists in the graph.
    """
    feather_file = "data/raw/proofread_connections_783.feather"
    
    # Skip test if data file is missing (common in CI if volumes aren't mounted)
    if not os.path.exists(feather_file):
        pytest.skip(f"Data file not found at {feather_file}")

    rows_to_load = 10_000

    # Run the ETL function
    load_connections_arrow(
        neo4j_driver,
        feather_path=feather_file,
        max_rows=rows_to_load,
        batch_size=2_000,
        clear_graph=True,
    )

    # 2. ASSERTION: Verify the data is actually in the database
    with neo4j_driver.session() as session:
        # Check Relationship Count
        result = session.run("MATCH ()-[r:CONNECTS_TO]->() RETURN count(r) as count")
        rel_record = result.single()
        
        # FIX: Explicitly assert record is not None before indexing
        assert rel_record is not None, "Query returned no results"
        rel_count = rel_record["count"]
        
        # Check Node Count
        node_result = session.run("MATCH (n:Neuron) RETURN count(n) as count")
        node_record = node_result.single()
        
        # FIX: Explicitly assert record is not None before indexing
        assert node_record is not None, "Query returned no results"
        node_count = node_record["count"]

    print(f"\nVerifying: Found {node_count} nodes and {rel_count} relationships.")

    assert rel_count > 0, "No relationships were created in Neo4j."
    assert node_count > 0, "No Neuron nodes were created in Neo4j."


def test_dashboard_summary_is_from_neo4j(neo4j_driver: Driver, tmp_path:pathLib.Path) -> None:
    """Prove the dashboard summary endpoint can be sourced from Neo4j.

    We insert a known (:DatasetSummary {id}) record into Neo4j, then call the
    dashboard summary function with its module-level source toggled to Neo4j.
    """

    dataset_id = "pytest-dashboard-summary"
    expected_summary = {
        "input": "pytest",
        "max_rows": 1,
        "total_rows": 1,
        "unique": {},
        "syn_count": {},
    }

    summary_path = tmp_path / "summary.json"
    summary_path.write_text(json.dumps(expected_summary), encoding="utf-8")

    push_summary_json_to_neo4j(
        neo4j_driver,
        summary_json_path=str(summary_path),
        dataset_id=dataset_id,
        clear_existing=True,
    )

    # Import here (after insert) so we can safely patch module-level constants.
    from src.app import dashboard as dashboard_module

    old_source = dashboard_module.SUMMARY_SOURCE
    old_id = dashboard_module.SUMMARY_DATASET_ID
    try:
        dashboard_module.SUMMARY_SOURCE = "neo4j"
        dashboard_module.SUMMARY_DATASET_ID = dataset_id

        got = dashboard_module.dataset_summary()
        assert got == expected_summary
    finally:
        dashboard_module.SUMMARY_SOURCE = old_source
        dashboard_module.SUMMARY_DATASET_ID = old_id

        with neo4j_driver.session() as session:
            session.run("MATCH (s:DatasetSummary {id: $id}) DETACH DELETE s", id=dataset_id)


def test_fastapi_up_and_neo4j_ok(neo4j_driver: Driver) -> None:
    """Smoke test: FastAPI responds and Neo4j is reachable."""

    from fastapi.testclient import TestClient

    from src.app.api import app

    client = TestClient(app)
    resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok"}

    # Confirm DB connectivity (uses the same driver fixture used elsewhere in CI).
    check_connection(neo4j_driver)
    neo4j_driver.verify_connectivity()