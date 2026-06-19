import asyncpg

from eth_fraud_detection.core.config import get_postgres_settings
from eth_fraud_detection.utils.logger import eth_logger


CREATE_FEATURES_TABLE = """
CREATE TABLE IF NOT EXISTS tx_features (
    tx_hash               TEXT PRIMARY KEY,
    from_address          TEXT        NOT NULL,
    value_eth             FLOAT,
    gas_price_gwei        FLOAT,
    nonce                 BIGINT,
    out_degree            INT,
    in_degree             INT,
    unique_counterparties INT,
    total_volume          FLOAT,
    avg_tx_value          FLOAT,
    anomaly_score         FLOAT,
    is_fraud              BOOLEAN     DEFAULT FALSE,
    created_at            TIMESTAMPTZ DEFAULT NOW()
)
"""


class PostgresSQLDb:
    def __init__(self):
        settings = get_postgres_settings()
        self._dsn = settings.dsn
        self._pool: asyncpg.Pool | None = None

    async def connect(self):
        self._pool = await asyncpg.create_pool(self._dsn)
        async with self._pool.acquire() as conn:
            await conn.execute(CREATE_FEATURES_TABLE)
        eth_logger.info("PostgresSQL connected and schema ensured.")

    async def close(self):
        if self._pool:
            await self._pool.close()

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    async def insert_features(self, features: dict) -> None:
        query = """
        INSERT INTO tx_features (
            tx_hash, from_address, value_eth, gas_price_gwei, nonce,
            out_degree, in_degree, unique_counterparties, total_volume, avg_tx_value
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (tx_hash) DO NOTHING
        """
        async with self._pool.acquire() as conn:
            await conn.execute(
                query,
                features["tx_hash"],
                features["from_address"],
                features["value_eth"],
                features["gasPrice_gwei"],
                features["nonce"],
                features["out_degree"],
                features["in_degree"],
                features["unique_counterparties"],
                features["total_volume"],
                features["avg_tx_value"],
            )

    async def insert_features_batch(self, records: list[dict]) -> None:
        try:
            rows = [
                (
                    r["tx_hash"], r["from_address"], r["value_eth"],
                    r["gasPrice_gwei"], r["nonce"], r["out_degree"],
                    r["in_degree"], r["unique_counterparties"],
                    r["total_volume"], r["avg_tx_value"],
                )
                for r in records
            ]
            query = """
            INSERT INTO tx_features (
                tx_hash, from_address, value_eth, gas_price_gwei, nonce,
                out_degree, in_degree, unique_counterparties, total_volume, avg_tx_value
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            ON CONFLICT (tx_hash) DO NOTHING
            """
            async with self._pool.acquire() as conn:
                await conn.executemany(query, rows)
            eth_logger.info(f"Inserted {len(rows)} feature rows.")
        except Exception as e:
            eth_logger.error(e)

    async def update_anomaly_score(self, tx_hash: str, score: float, is_fraud: bool) -> None:
        query = """
        UPDATE tx_features
        SET anomaly_score = $1, is_fraud = $2
        WHERE tx_hash = $3
        """
        async with self._pool.acquire() as conn:
            await conn.execute(query, score, is_fraud, tx_hash)

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    async def get_by_tx_hash(self, tx_hash: str) -> dict | None:
        query = "SELECT * FROM tx_features WHERE tx_hash = $1"
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(query, tx_hash)
        return dict(row) if row else None

    async def get_unscored(self, batch_size: int = 1000) -> list[dict]:
        query = """
        SELECT * FROM tx_features
        WHERE anomaly_score IS NULL
        LIMIT $1
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, batch_size)
        return [dict(r) for r in rows]

    async def get_fraud(self, limit: int = 1000) -> list[dict]:
        query = """
        SELECT * FROM tx_features
        WHERE is_fraud = TRUE
        ORDER BY anomaly_score DESC
        LIMIT $1
        """
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(query, limit)
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    async def delete_by_tx_hash(self, tx_hash: str) -> None:
        query = "DELETE FROM tx_features WHERE tx_hash = $1"
        async with self._pool.acquire() as conn:
            await conn.execute(query, tx_hash)

    async def delete_by_address(self, address: str) -> None:
        query = "DELETE FROM tx_features WHERE from_address = $1"
        async with self._pool.acquire() as conn:
            await conn.execute(query, address)