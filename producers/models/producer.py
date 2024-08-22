"""Producer base-class providing common utilites and functionality"""

import logging
import time


from confluent_kafka import avro
from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka.avro import AvroProducer

logger = logging.getLogger(__name__)


class Producer:
    """Defines and provides common functionality amongst Producers"""

    # Tracks existing topics across all Producer instances
    existing_topics = set([])

    def __init__(
        self,
        topic_name,
        key_schema,
        value_schema=None,
        num_partitions=1,
        num_replicas=1,
    ):
        """Initializes a Producer object with basic settings"""
        self.topic_name = topic_name
        self.key_schema = key_schema
        self.value_schema = value_schema
        self.num_partitions = num_partitions
        self.num_replicas = num_replicas

        # This dictionary holds the configuration properties for the message broker.
        # It includes the following keys:
        # - BROKER_URL: The URL of the Kafka broker. This is where messages are sent to be stored and later consumed.
        #   The protocol `PLAINTEXT://` indicates that the connection is not encrypted.
        # - SCHEMA_REGISTRY_URL: The URL of the schema registry. This service is used for storing and retrieving the schemas
        #   of the messages sent to the Kafka broker.
        self.broker_properties = {
            "BROKER_URL": "PLAINTEXT://localhost:9092",
            "SCHEMA_REGISTRY_URL": "http://localhost:8081",
        }

        # If the topic does not already exist, try to create it
        if self.topic_name not in Producer.existing_topics:
            self.create_topic()
            Producer.existing_topics.add(self.topic_name)

        # Initialize an AvroProducer instance, which is used for producing messages that adhere to Avro schemas.
        #
        # The AvroProducer is configured with the following parameters:
        # 1. bootstrap.servers: A configuration specifying the Kafka broker(s) to connect to. This is obtained from the
        #    `broker_properties` dictionary using the key "BROKER_URL". It indicates the address of the Kafka broker.
        #
        # 2. schema_registry: This parameter is an instance of CachedSchemaRegistryClient, initialized with the URL of the
        #    schema registry. The schema registry URL is also obtained from the `broker_properties` dictionary using the key
        #    "SCHEMA_REGISTRY_URL". The schema registry is used to store and retrieve Avro schemas for the messages being
        #    produced.
        #
        # 3. default_key_schema and default_value_schema: These parameters specify the Avro schemas for the keys and values of
        #    the messages, respectively. These schemas are used to serialize the messages' keys and values into Avro format
        #    before they are sent to the Kafka broker.
        self.producer = AvroProducer(
            {
                "bootstrap.servers": self.broker_properties["BROKER_URL"],
            },
            schema_registry=avro.CachedSchemaRegistryClient(
                {"url": self.broker_properties["SCHEMA_REGISTRY_URL"]}
            ),
            default_key_schema=key_schema,
            default_value_schema=value_schema,
        )

    def create_topic(self):
        """Creates the producer topic if it does not already exist"""

        # Initialize the AdminClient with the broker's URL
        client = AdminClient(
            {"bootstrap.servers": self.broker_properties["BROKER_URL"]}
        )
        topic_metadata = client.list_topics(timeout=5)

        # Check to see if the given topic exists
        if self.topic_name in topic_metadata.topics:
            logger.info(f"topic exits {self.topic_name}")
            return

        # Define the new topic with its name, number of partitions, and replication factor
        topic = NewTopic(
            topic=self.topic_name,
            num_partitions=self.num_partitions,
            replication_factor=self.num_replicas,
        )

        # Attempt to create the topic
        futures = client.create_topics([topic])

        # Iterate through the futures for each topic creation request
        for topic, future in futures.items():
            try:
                future.result()
                logger.info(f"topic {topic} created")
            except Exception as e:
                logger.fatal(f"failed to create topic {topic}: {e}")
                raise

    def time_millis(self):
        """Use this function to get the key for Kafka Events"""
        return int(round(time.time() * 1000))

    def close(self):
        """Prepares the producer for exit by cleaning up the producer"""
        self.producer.flush()
