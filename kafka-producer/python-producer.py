import datetime
import time
import random
import schedule
import logging
from json import dumps
from faker import Faker
from kafka import KafkaProducer


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

KAFKA_NODES = "kafka:9092"
TOPIC = "weather"
faker = Faker()

producer = KafkaProducer(
    bootstrap_servers=KAFKA_NODES,
    value_serializer=lambda x: dumps(x).encode("utf-8"),
    retries=10,
    retry_backoff_ms=3000,
    acks="all"
)

def gen_data():
    try:
        data = {
            "city": faker.city(),
            "temperature": random.uniform(10.0,100.0)
        }
        future = producer.send(TOPIC,data)
        metadata = future.get(timeout=10)
        logging.info(
            f"Sent -> {data} "
            f"(partition={metadata.partition}, offset={metadata.offset})"
        )
    except Exception:
        logging.exception("Failed sending message")


if __name__=="__main__":
    logging.info("Kafka Producer Started")
    gen_data()
    schedule.every(10).seconds.do(gen_data)
    while True:
        schedule.run_pending()
        time.sleep(1)
        
        
        
# kafka_nodes = "kafka:9092"
# my_topic = "weather"

# def gen_data():
#     faker = Faker()
#     prod = KafkaProducer(bootstrap_servers=kafka_nodes, value_serializer=lambda x:dumps(x).encode('utf-8'))
#     my_data = {
#         'city': faker.city(),
#         'temperature': random.uniform(10.0, 100.0)
#     }
#     prod.send(topic=my_topic, value=my_data)
#     prod.flush()

# if __name__ == '__main__':
#     gen_data()
#     schedule.every(10).seconds.do(gen_data)
    
#     while True:
#         schedule.run_pending()
#         time.sleep(0.5)
    
