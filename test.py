"""
    Testing of the module in CI/CD pipeline of docker-compose.yaml

    Uses pytest to run tests on the database connection and ETL process. 
"""
import os
from typing import Generator
import pytest
from neo4j import Driver

from app.db import get_driver, close_driver, test_connection
from app.etl import load_connections_arrow

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
        test_connection(neo4j_driver)
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