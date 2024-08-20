"""Defines trends calculations for stations"""

import logging

import faust


logger = logging.getLogger(__name__)


# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str


# Define a Faust Stream that ingests data from the Kafka Connect stations topic and
# places it into a new topic with only the necessary information.
app = faust.App("stations-stream", broker="kafka://localhost:9092", store="memory://")
topic = app.topic("postgre_connect_stations", value_type=Station)
out_topic = app.topic("stations.table", partitions=1)

table = app.Table(
    "transformed_stations_table",
    default=TransformedStation,
    partitions=1,
    changelog_topic=out_topic,
)


@app.agent(topic)
async def transform(stations):
    # Refactor line determination to be more scalable and readable
    line_colors = {"red": station.red, "blue": station.blue, "green": station.green}
    line = next((color for color, active in line_colors.items() if active), "")

    try:
        # Update the table with the transformed station data
        table[station.station_id] = TransformedStation(
            station_id=station.station_id,
            station_name=station.station_name,
            order=station.order,
            line=line,
        )
    except Exception as e:
        # Log the error or handle it as needed
        print(f"Error processing station {station.station_id}: {e}")


if __name__ == "__main__":
    app.main()
