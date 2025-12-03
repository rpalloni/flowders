import logging
import time
from .broker import initiate_stream

APP_NAME = 'OrdersAggregator'
KAFKA_TOPIC = 'orders'
KAFKA_BROKER = 'kafka1:9092,kafka2:9092,kafka3:9092'

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    logging.info('Waiting Kafka to wake up...')
    time.sleep(30)
    logging.info('Start streaming...')
    initiate_stream(KAFKA_BROKER, KAFKA_TOPIC)