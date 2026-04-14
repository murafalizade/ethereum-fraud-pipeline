import asyncio
import networkx as nx
import pandas as pd
from neo4j import AsyncGraphDatabase
from karateclub.node_embedding.neighbourhood import RandNE

# Use bolt://localhost:7687 if running the script on your host machine
NEO4J_URI = "bolt://localhost:7687"
NEO4J_USER = "neo4j"
NEO4J_PASS = "password123"


class GraphDb:
    def __init__(self):
        self.driver = AsyncGraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASS))

    async def close(self):
        await self.driver.close()

    async def insert_transaction(self, tx):
        query = """
            MERGE (from:Address {address: $from})
            MERGE (to:Address {address: $to})

            MERGE (tx:Transaction {hash: $hash})
            SET tx.value_eth = $value_eth,
                tx.gasPrice_gwei = $gasPrice_gwei,
                tx.blockNumber = $blockNumber,
                tx.nonce = $nonce

            MERGE (from)-[:SENT]->(tx)
            MERGE (tx)-[:TO]->(to)
            """
        async with self.driver.session() as session:
            await session.run(query, tx)

    async def generate_and_sync_sigs(self):
        """
        Pulls graph topology, calculates embeddings with RandNE,
        and updates the nodes in Neo4j with the signature.
        """
        async with self.driver.session() as session:
            query = """
            MATCH (a:Address)-[:SENT]->(t:Transaction)-[:TO]->(b:Address) 
            RETURN id(a) as source, id(b) as target
            """
            result = await session.run(query)

            records = []
            async for record in result:
                records.append({"source": record["source"], "target": record["target"]})

            if not records:
                print("No data found to process.")
                return

            df = pd.DataFrame(records)
            g_raw = nx.from_pandas_edgelist(df, 'source', 'target')

            nodes = list(g_raw.nodes())
            mapping = {node: i for i, node in enumerate(nodes)}
            g = nx.relabel_nodes(g_raw, mapping)

            model = RandNE(dimensions=32)
            model.fit(g)
            embeddings = model.get_embedding()

            batch_data = [
                {"id": nodes[i], "sig": embeddings[i].tolist()}
                for i in range(len(nodes))
            ]

            write_query = """
            UNWIND $data as item
            MATCH (a) WHERE id(a) = item.id
            SET a.signature = item.sig
            """
            await session.run(write_query, data=batch_data)
            print(f"Successfully updated signatures for {len(batch_data)} nodes.")


async def main():
    db = GraphDb()
    try:
        await db.generate_and_sync_sigs()
    finally:
        await db.close()


if __name__ == '__main__':
    asyncio.run(main())