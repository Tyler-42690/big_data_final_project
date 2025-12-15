'''
Docstring for src.app.db
'''
import os
import logging
from neo4j import Driver, GraphDatabase


logging.basicConfig(filename='output.log',
    filemode='a', #Append mode               
    level=logging.WARNING,         
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def get_driver() -> Driver:
    #use local host for local excution, use NEO4J_URI when using docker
    uri = os.getenv("NEO4J_URI", "bolt://localhost:7687")
    #uri = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
    user = os.getenv("NEO4J_USER", "neo4j")
    password = os.getenv("NEO4J_PASSWORD", "example_password")

    driver = GraphDatabase.driver(uri, auth=(user, password))
    return driver


def close_driver(driver:Driver) -> None:
    driver.close()


def check_connection(driver:Driver) -> None:
    try:
        with driver.session() as session:

            record = session.run("RETURN 1 AS ok").single(strict=True)
            print("Neo4j test query result:", record["ok"])

    except Exception as e:
        logging.error("Error connecting to Neo4j: %s", str(e))
        raise
