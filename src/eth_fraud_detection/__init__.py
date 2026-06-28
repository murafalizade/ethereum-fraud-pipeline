import asyncio

from eth_fraud_detection.apps.consumer.kafka_eth_consumer import KafkaEthConsumer
from eth_fraud_detection.apps.producer.kafka_eth_producer import KafkaEthProducer


async def main() -> None:
    eth_producer = KafkaEthProducer()
    eth_consumer = KafkaEthConsumer()
    await asyncio.gather(
        eth_producer.listen(),
        eth_consumer.process()
    )

if __name__ == "__main__":
    asyncio.run(main())