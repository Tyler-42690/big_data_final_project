from neo4j import Driver, GraphDatabase
import os


def get_driver() -> Driver:
    #use local host for local excution, use NEO4J_URI when using docker
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    #uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "example_password")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver


def close_driver(driver):
    driver.close()


def test_connection(driver):
    with driver.session() as session:
        record = session.run("RETURN 1 AS ok").single()
        print("Neo4j test query result:", record["ok"])
