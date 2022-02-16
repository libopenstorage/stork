import json
import os
import signal
import logging
import sys
import time

import names
import requests
from kafka import KafkaProducer

FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format=FORMAT)

servers = os.getenv('KAFKA_BOOTSTRAP_SERVERS', "localhost:9092")
topic = os.getenv('KAFKA_TOPIC', "")
connector_server = os.getenv('CONNECTOR_SERVER', "")
elasticsearch_server = os.getenv('ES_SERVER', "")

producer = KafkaProducer(bootstrap_servers=servers, value_serializer=lambda m: json.dumps(m).encode('ascii'))


def signal_handler(sig, frame):
    logging.info("shutting down...")
    producer.flush()
    sys.exit(0)


def on_send_success(record_metadata):
    logging.info("topic: %s partition: %s offset: %s", record_metadata.topic, record_metadata.partition,
                 record_metadata.offset)


def on_send_error(excp):
    logging.error('error on sending msg', exc_info=excp)


signal.signal(signal.SIGINT, signal_handler)
signal.signal(signal.SIGTERM, signal_handler)


def post_es_connector():
    data = {
      "name": f"{topic}-connector",
      "config": {
        "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
        "connection.url": f"http://{elasticsearch_server}",
        "tasks.max": "5",
        "topics": f"{topic}",
        "type.name": "_doc",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false",
        "schema.ignore": "true",
        "key.ignore": "true",
        "errors.tolerance": "all",
        "flush.timeout.ms": "600000",
        # "linger.ms": "2000",
        "max.in.flight.requests": "1000",
        "read.timeout.ms": "604800000",
     }
    }
    resp = requests.post(f"http://{connector_server}/connectors", json=data)
    if resp.status_code != 200:
        logging.error(f"{resp.status_code} {resp.text}")
    else:
        logging.info(resp.text)


post_es_connector()

while True:
    producer.send(topic, {'name': names.get_full_name()}).add_callback(on_send_success).add_errback(on_send_error)
