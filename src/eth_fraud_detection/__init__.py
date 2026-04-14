from eth_fraud_detection.apps.consumer.kafka_eth_consumer import KafkaEthConsumer
from eth_fraud_detection.apps.producer.kafka_eth_producer import KafkaEthProducer


def main() -> None:
    eth_producer = KafkaEthProducer()
    eth_consumer = KafkaEthConsumer()
    eth_producer.listen()
    eth_consumer.process()
    print("Hello from eth-fraud-detection!")
