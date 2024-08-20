"""Configures a Kafka Connector for Postgres Station data"""
import json
import logging

import requests


logger = logging.getLogger(__name__)


KAFKA_CONNECT_URL = "http://localhost:8083/connectors"
CONNECTOR_NAME = "stations"

def configure_connector():
    """Starts and configures the Kafka Connect connector"""
    logging.debug("creating or updating kafka connect connector...")

    resp = requests.get(f"{KAFKA_CONNECT_URL}/{CONNECTOR_NAME}")
    if resp.status_code == 200:
        logging.debug("connector already created skipping recreation")
        return

    logger.info("connector code not completed skipping connector creation")

    # Send a POST request to the Kafka Connect URL to create a new connector.
    resp = requests.post(
        KAFKA_CONNECT_URL,
        headers={
            "Content-Type": "application/json"
        },
        data=json.dumps({
            "name": CONNECTOR_NAME,
            "config": {
                "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
                "key.converter": "org.apache.kafka.connect.json.JsonConverter",
                "key.converter.schemas.enable": "false",
                "value.converter": "org.apache.kafka.connect.json.JsonConverter",
                "value.converter.schemas.enable": "false",
                "batch.max.rows": "500",
                "connection.url": "jdbc:postgresql://localhost:5432/cta",
                "connection.user": "cta_admin",
                "connection.password": "chicago",
                "table.whitelist": "stations",
                "mode": "incrementing",
                "incrementing.column.name": "stop_id",
                "topic.prefix": "postgre_connect_",
                "poll.interval.ms": "10000",
            }
        }),
    )

    try:
        resp.raise_for_status()
        logging.debug("connector created successfully")
    except:
        logging.debug("connector creation failed")
        logger.error(
            "Failed to create connector: status code %s, message: %s",
            resp.status_code,
            resp.content,
        )


if __name__ == "__main__":
    configure_connector()
