from app.db import get_driver, test_connection, close_driver
from app.etl import load_connections_arrow
from app.graph_query import (
    get_postsynaptic_partners,
    get_presynaptic_partners,
    get_two_hop_upstream,
    get_two_hop_downstream,
)
from app.simulate import simulate_silence, simulate_boost


def main():
    driver = get_driver()
    print("Connected to Neo4j.")
    test_connection(driver)

    load_connections_arrow(
        driver,
        "data/raw/proofread_connections_783.feather",
        max_rows=100_000,     # 
        batch_size=10_000,
        clear_graph=True,
    )

    with driver.session() as session:
        record = session.run("""
            MATCH (n:Neuron)-[r:CONNECTS_TO]->()
            RETURN n.root_id AS id, count(r) AS out_deg
            ORDER BY out_deg DESC
            LIMIT 1
        """).single()

        if record is None:
            print("No Neuron nodes with outgoing edges found.")
            close_driver(driver)
            return

        target_id = record["id"]

    print(f"\nUsing target neuron with highest out-degree (in this subset): {target_id}")


    postsyn = get_postsynaptic_partners(driver, target_id)
    presyn = get_presynaptic_partners(driver, target_id)

    print(f"\nDirect postsynaptic partners of {target_id}: (top 5)")
    for r in postsyn[:5]:
        print(r)

    print(f"\nDirect presynaptic partners of {target_id}: (top 5)")
    for r in presyn[:5]:
        print(r)

    print(f"\nTwo-hop upstream chains (pre-of-pre) for {target_id}: (top 5)")
    for r in get_two_hop_upstream(driver, target_id)[:5]:
        print(r)

    print(f"\nTwo-hop downstream chains (post-of-post) for {target_id}: (top 5)")
    for r in get_two_hop_downstream(driver, target_id)[:5]:
        print(r)

    print(f"\nSimulate silencing neuron {target_id}: (top 5 edges)")
    for r in simulate_silence(driver, target_id)[:5]:
        print(r)

    print(f"\nSimulate boosting neuron {target_id} by factor 2.0: (top 5 edges)")
    for r in simulate_boost(driver, target_id, factor=2.0)[:5]:
        print(r)

    close_driver(driver)


if __name__ == "__main__":
    main()
