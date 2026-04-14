import json
from aiokafka import AIOKafkaConsumer

from eth_fraud_detection.adapters.graph_db import GraphDb
from eth_fraud_detection.core.constants import TRANSACTION_WRITER_TOPIC


class KafkaEthConsumer:
    def __init__(self):
        self.consumer = AIOKafkaConsumer(
            TRANSACTION_WRITER_TOPIC,
            bootstrap_servers="localhost:9092",
            value_deserializer=lambda v: json.loads(v.decode('utf-8')),
            enable_auto_commit=True,
        )
        self.graph = GraphDb()

    async def process(self):
        await self.consumer.start()

        try:
            async for message in self.consumer:
                tx = message.value
                response = {
                    'hash': tx['hash'],
                    'from': tx['from'].lower(),
                    'to': tx['to'].lower(),
                    'value_eth': int(tx["value"], 16) / 1e18,
                    'gasPrice_gwei': int(tx['gasPrice'], 16) / 1e9,
                    'blockNumber': int(tx["blockNumber"], 16),
                    'nonce': int(tx['nonce'], 16),
                }

                await self.graph.insert_transaction(response)

        finally:
            await self.consumer.stop()
