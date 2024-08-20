"""Creates a turnstile data producer"""

import logging
from pathlib import Path

from confluent_kafka import avro

from models.producer import Producer
from models.turnstile_hardware import TurnstileHardware


logger = logging.getLogger(__name__)


class Turnstile(Producer):
    key_schema = avro.load(f"{Path(__file__).parents[0]}/schemas/turnstile_key.json")
    value_schema = avro.load(
        f"{Path(__file__).parents[0]}/schemas/turnstile_value.json"
    )

    def __init__(self, station):
        """Create the Turnstile"""
        station_name = (
            station.name.lower()
            .replace("/", "_and_")
            .replace(" ", "_")
            .replace("-", "_")
            .replace("'", "")
        )

        # Construct the topic name dynamically based on the station name.
        topic_name = f"turnstile.station.{station_name}"

        # Call the Producer's constructor, passing in the topic name, key schema, and value schema.
        super().__init__(
            topic_name,
            key_schema=Turnstile.key_schema,  # Avro schema for the message key ensures consistency in message format.
            value_schema=Turnstile.value_schema,  # Avro schema for the message value ensures data integrity and structure.
            num_partitions=1,  # Number of partitions for the Kafka topic. Adjust for scalability and performance.
            num_replicas=1,  # Number of replicas for fault tolerance. Increase to ensure data availability.
        )
        self.station = station
        self.turnstile_hardware = TurnstileHardware(station)

    def run(self, timestamp, time_step):
        """Simulates riders entering through the turnstile."""
        # Retrieve the number of entries from the turnstile hardware for the given time step.
        num_entries = self.turnstile_hardware.get_entries(timestamp, time_step)

        # Iterate over each entry recorded in the time step and produce a message
        # to the Kafka topic for each entry.
        for _ in range(num_entries):
            self.producer.produce(
                topic=self.topic_name,  # The Kafka topic to which the message will be sent.
                key_schema=self.key_schema,  # The schema for the message key.
                value_schema=self.value_schema,  # The schema for the message value.
                key={
                    "timestamp": self.time_millis()  # The current time in milliseconds as the message key.
                },
                value={
                    "station_id": self.station.station_id,  # The station ID for the turnstile.
                    "station_name": self.station.name,  # The name of the station.
                    "line": self.station.color.name,  # The line color of the station.
                },
            )
