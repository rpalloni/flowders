import json
import time
import random
import logging
from kafka import KafkaProducer
from murmurhash2 import murmurhash2
from .producer import generate_order

CUSTOMER_IDS = [
    "CUS_JMUUMX57",
    "CUS_Q6Z8GZNH",
    "CUS_FCPCPQOK",
    "CUS_HB6C22TP",
    "CUS_RCTQC56L",
    "CUS_O2LV2E0Q",
    "CUS_98LKLSKX",
    "CUS_IFNDUZGA",
    "CUS_5W0GO8GP",
    "CUS_1XE8U8UW"
]

def partition_routing(key_bytes, all_partitions, available_partitions):
    '''
    partitioner: Callable used to determine which partition each message is assigned to.
    
    Define a custom partitioner implementing direct mapping to force one ID per partition (0-9) 
    as default hashing has collision on some partitions due to small CUSTOMER_IDS set for this example
    - non-None keys: default hashing using murmur2 algorithm
    - None keys: round-robin
    '''
    if key_bytes is None:
        return random.choice(available_partitions)
    customer_id = key_bytes.decode("utf-8")
    if customer_id in CUSTOMER_IDS:
        idx = CUSTOMER_IDS.index(customer_id)
        return all_partitions[idx]
    # fallback for unknown IDs
    hash_value = murmurhash2(key_bytes, 123)
    return all_partitions[hash_value % len(all_partitions)]

    

def publish_to_kafka(producer, topic, data):
    """Sends data to a Kafka topic. convert key and value into byte format (as Kafka works in bytes)."""
    logging.info(f'Sending data to brokers - topic: {topic}, message: {data}')
    
    customer_id = json.loads(data)['customer_id']
    producer.send(topic, key=customer_id, value=data)

    # cus_id = int(json.loads(data)['customer_id'])
    # producer.send(topic, partition=cus_id, value=data)    # explicitly set partition destination
    
    # producer.send(topic, value=data)                      # round-robin without key
    producer.flush() #  Ensures all buffered messages are sent to Kafka


def initiate_stream(broker, topic):
    """Initiates the process to stream orders data to Kafka."""
    producer = KafkaProducer(
        bootstrap_servers= broker,
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: v.encode("utf-8"),
        partitioner=partition_routing, # comment out to have hashing default partitioning - hash(key) % partitions
        acks="all", 
        # acks 0: Producer will not wait for any acknowledgment from the server
        # acks 1: (default) Wait for LEADER broker to write the record to its local log only
        # acks all: Wait for the full set of in-sync replicas to write the record
    )
    try:
        while True:
            order = generate_order(CUSTOMER_IDS)
            publish_to_kafka(producer, topic, order)
            time.sleep(random.randint(1,10))
    finally:
        logging.info('Closing broker connection...')
        producer.close()