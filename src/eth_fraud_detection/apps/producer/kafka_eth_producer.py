import json
import os
import websockets
from aiokafka import AIOKafkaProducer

from eth_fraud_detection.core.constants import TRANSACTION_WRITER_TOPIC

# Use Environment Variables for security
API_KEY = os.getenv("ALCHEMY_ETH_API_KEY", "z6o_inQRnUeeb8QjhES_G")
URL = f'wss://eth-mainnet.g.alchemy.com/v2/{API_KEY}'

class KafkaEthProducer:
    def __init__(self):
        self.producer = AIOKafkaProducer(
        bootstrap_servers='localhost:9092',
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        compression_type="gzip"
    )

    async def listen(self):
        await self.producer.start()
        print("Kafka Producer Started. Listening to Ethereum...")

        try:
            async with websockets.connect(URL) as ws:
                await ws.send(json.dumps({
                    "jsonrpc": "2.0",
                    "id": 1,
                    "method": "eth_subscribe",
                    "params": ["alchemy_minedTransactions"]
                }))

                async for message in ws:
                    data = json.loads(message)

                    if data.get("method") != "eth_subscription":
                        continue

                    tx = data["params"]["result"]['transaction']

                    # Only Account-to-Account (No Smart Contracts)
                    if tx.get("input") != "0x" or not tx.get("to"):
                        continue

                    await self.producer.send_and_wait(TRANSACTION_WRITER_TOPIC, tx)

        except Exception as e:
            print(f"Error: {e}")
        finally:
            await self.producer.stop()
            print("Producer stopped.")
