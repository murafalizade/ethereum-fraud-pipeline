import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Neo4jConfig:
    uri: str
    user: str
    password: str


@dataclass(frozen=True)
class PostgresConfig:
    host: str
    port: int
    database: str
    user: str
    password: str

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


@dataclass(frozen=True)
class PipelineConfig:
    embedding_dim: int
    neo4j: Neo4jConfig
    postgres: PostgresConfig

    @classmethod
    def from_env(cls) -> "PipelineConfig":
        return cls(
            embedding_dim=int(os.getenv("EMBEDDING_DIM", "32")),
            neo4j=Neo4jConfig(
                uri=os.getenv("NEO4J_URI", "bolt://localhost:7687"),
                user=os.getenv("NEO4J_USER", "neo4j"),
                password=os.getenv("NEO4J_PASSWORD", "password123"),
            ),
            postgres=PostgresConfig(
                host=os.getenv("POSTGRES_HOST", "localhost"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                database=os.getenv("POSTGRES_DB", "eth_fraud"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "postgres"),
            ),
        )
