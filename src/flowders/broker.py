import json
import uuid
import time
import random
import logging
from kafka import KafkaProducer
from .producer import generate_order


def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic. convert key and value into byte format (as Kafka works in bytes)."""
    customer_id = str(json.loads(data)['customer_id']).encode('utf-8')
    logging.info(f'Sending data to brokers - topic: {topic}, message: {data}')
    producer.send(topic, key=customer_id , value=data.encode('utf-8'))
    producer.flush() #  Ensures all buffered messages are sent to Kafka


def initiate_stream(broker, topic):
    """Initiates the process to stream orders data to Kafka."""
    producer = KafkaProducer(bootstrap_servers= broker)
    try:
        while True:
            order = generate_order()
            publish_to_kafka(producer, topic, order)
            time.sleep(random.randint(1,10))
    finally:
        logging.info('Closing broker connection...')
        producer.close()