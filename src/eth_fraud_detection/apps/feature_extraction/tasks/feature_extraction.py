import asyncio

from eth_fraud_detection.adapters.graph_db import GraphDb
from eth_fraud_detection.adapters.postgresql_db import PostgresSQLDb
from eth_fraud_detection.utils.logger import eth_logger


async def extract_features():
    graph_db = GraphDb()
    postgres_sql = PostgresSQLDb()
    await postgres_sql.connect()
    try:
        total = await graph_db.get_count_unextracted_tx_hashes()
        eth_logger.info(total)
        processed = 0
        while processed < total:
            hashes = await graph_db.get_unextracted_tx_hashes()
            features = await asyncio.gather(*[graph_db.get_tx_features(tx) for tx in hashes],
                                            return_exceptions = True)
            valid = []
            for tx, result in zip(hashes, features):
                if isinstance(result, dict):
                    valid.append(result)
                else:
                    eth_logger.warning(f"Dead-lettered tx {tx}: {result}")
            if valid:
                await postgres_sql.insert_features_batch(valid)
            processed += len(hashes)
        eth_logger.info("Successfully all transactions are moved!")
    finally:
        await postgres_sql.close()
        await graph_db.close()

if __name__ == "__main__":
    asyncio.run(extract_features())