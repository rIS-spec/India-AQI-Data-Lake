# PURPOSE — producer.py sends the AQI data your FastAPI fetched into a Kafka topic, so the rest of the pipeline can consume it.


import json
import logging
from kafka import KafkaProducer
from config import settings as settings_module

logger = logging.getLogger(__name__)



# create_producer() -  connects to Kafka broker, returns a producer object 
def create_producer():
    return KafkaProducer(
        bootstrap_servers=settings_module.KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8")
    )



# send_to_kafka() - takes a record, sends it to the topic, handles errors 
def send_to_kafka(producer, record: dict):
    topic = settings_module.KAFKA_TOPIC_AQI
    producer.send(topic, value=record)
    logger.info(f"Sent record for city: {record.get('city')}")



# produce_aqi_batch() - loops over all city records and calls send_to_kafka() for each 
def produce_aqi_batch(records: list[dict]):
    producer = create_producer()
    for record in records:
        send_to_kafka(producer, record)
    producer.flush()
    logger.info(f"Batch complete. {len(records)} records sent to Kafka.")





# HOW producer.py WORKS:
# .env loads KAFKA_BOOTSTRAP_SERVERS + KAFKA_TOPIC_AQI
# → create_producer() connects to Kafka at localhost:9092
# → produce_aqi_batch() receives list of 10 city AQI dicts
# → loops through each city dict and calls send_to_kafka 
# → send_to_kafka() pushes each dict to topic "aqi-raw-data"
# → value_serializer converts dict to bytes automatically
# → producer.flush() confirms all 10 messages are delivered
# → Kafka holds the messages in "aqi-raw-data" topic
# → bronze layer will read from this topic in Phase 3


# aqi_fetcher.py        → fetches live AQI for 10 cities from OpenWeatherMap
#         ↓
# aqi_schema.py         → validates each city's data shape (AQIRecord)
#         ↓
# main.py               → FastAPI exposes /fetch-aqi endpoint, calls aqi_fetcher
#         ↓
# producer.py           → takes the 10 city records, sends to Kafka topic "aqi-raw-data"
#         ↓
# Kafka (Docker)        → holds the messages in the topic like a queue
#         ↓
# bronze layer(Phase 3) → PySpark reads from Kafka, writes raw data to S3
