import networkx as nx
import pandas as pd
from neo4j import AsyncGraphDatabase
from karateclub.node_embedding.neighbourhood import RandNE

from eth_fraud_detection.core.config import get_neo4j_settings
from eth_fraud_detection.utils.logger import eth_logger


class GraphDb:
    def __init__(self):
        settings = get_neo4j_settings()
        self.driver = AsyncGraphDatabase.driver(settings.uri, auth=(settings.user, settings.password))

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
            eth_logger.info(f"Successfully updated signatures for {len(batch_data)} nodes.")

    async def get_tx_features(self, tx_hash: str) -> dict | None:
        query = """
        MATCH (from:Address)-[:SENT]->(tx:Transaction {hash: $hash})-[:TO]->(to:Address)
        WHERE tx.is_extracted IS NULL
        SET tx.is_extracted = true
        RETURN
            tx.hash                                                             AS tx_hash,
            from.address                                                        AS from_address,
            tx.value_eth                                                        AS value_eth,
            tx.gasPrice_gwei                                                    AS gasPrice_gwei,
            tx.nonce                                                            AS nonce,
            size([(from)-[:SENT]->() | 1])                                     AS out_degree,
            size([()-[:TO]->(from) | 1])                                       AS in_degree,
            size([(from)-[:SENT]->(t)-[:TO]->(x) | x])                        AS unique_counterparties,
            reduce(s = 0.0, t IN [(from)-[:SENT]->(t) | t.value_eth] | s + t) AS total_volume,
            reduce(s = 0.0, t IN [(from)-[:SENT]->(t) | t.value_eth] | s + t)
                / nullif(size([(from)-[:SENT]->() | 1]), 0)                   AS avg_tx_value
        """
        try:
            async with self.driver.session() as session:
                result = await session.run(query, hash=tx_hash)
                record = await result.single()

            if record is None:
                return None

            return {
                "tx_hash":               record["tx_hash"],
                "from_address":          record["from_address"],
                "value_eth":             record["value_eth"],
                "gasPrice_gwei":         record["gasPrice_gwei"],
                "nonce":                 record["nonce"],
                "out_degree":            record["out_degree"],
                "in_degree":             record["in_degree"],
                "unique_counterparties": record["unique_counterparties"],
                "total_volume":          record["total_volume"],
                "avg_tx_value":          record["avg_tx_value"],
            }
        except Exception as e:
            eth_logger.error(msg = f"Failed to get tx_hash for {tx_hash}")
            raise e

    async def get_unextracted_tx_hashes(self, batch_size: int = 1000) -> list[str]:
        query = """
        MATCH (tx:Transaction)
        WHERE tx.is_extracted IS NULL
        RETURN tx.hash AS tx_hash
        LIMIT $batch_size
        """
        async with self.driver.session() as session:
            result = await session.run(query, batch_size=batch_size)
            return [record["tx_hash"] async for record in result]
        return None

    async def get_count_unextracted_tx_hashes(self) -> int:
        query = """
        MATCH (tx:Transaction)
        WHERE tx.is_extracted IS NULL
        RETURN tx.hash AS tx_hash
        """
        async with self.driver.session() as session:
            result = await session.run(query)
            return len(await result.values())