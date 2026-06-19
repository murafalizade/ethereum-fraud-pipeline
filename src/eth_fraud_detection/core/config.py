from functools import lru_cache

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseSetting(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


class Neo4jSettings(BaseSetting):
    model_config = SettingsConfigDict(env_prefix="NEO4J_")

    uri: str = Field(default="bolt://localhost:7687")
    user: str = Field(default="neo4j")
    password: str = Field(default="password123")


class PostgresSettings(BaseSetting):
    model_config = SettingsConfigDict(env_prefix="POSTGRES_")

    host: str = Field(default="localhost")
    port: int = Field(default=5432)
    database: str = Field(default="eth_fraud", alias="POSTGRES_DB")
    user: str = Field(default="postgres")
    password: str = Field(default="postgres")

    @property
    def dsn(self) -> str:
        return f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"


class CommonSettings(BaseSetting):
    alchemy_eth_api_key: str = Field(default="ALCHEMY_ETH_API_KEY", alias="ALCHEMY_ETH_API_KEY")


@lru_cache
def get_neo4j_settings() -> Neo4jSettings:
    return Neo4jSettings()


@lru_cache
def get_postgres_settings() -> PostgresSettings:
    return PostgresSettings()


@lru_cache
def get_common_settings() -> CommonSettings:
    return CommonSettings()