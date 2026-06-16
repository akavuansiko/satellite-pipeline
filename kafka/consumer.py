import json
import duckdb
import os
from kafka import KafkaConsumer
from datetime import datetime

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "satellite_positions"
DB_PATH = os.path.join(os.path.dirname(__file__), '..', 'db', 'satellites.duckdb')

def init_db(con):
    con.execute("""
        CREATE TABLE IF NOT EXISTS satellite_positions (
            name        VARCHAR,
            norad_id    INTEGER,
            latitude    DOUBLE,
            longitude   DOUBLE,
            altitude_km DOUBLE,
            timestamp   TIMESTAMP
        )
    """)
    # Vue analytique ELT sur les positions
    con.execute("""
        CREATE OR REPLACE VIEW analytics_positions AS
        SELECT
            name,
            norad_id,
            AVG(altitude_km)    AS avg_altitude_km,
            MIN(altitude_km)    AS min_altitude_km,
            MAX(altitude_km)    AS max_altitude_km,
            COUNT(*)            AS total_records,
            MAX(timestamp)      AS last_seen
        FROM satellite_positions
        GROUP BY name, norad_id
    """)

def run():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = duckdb.connect(DB_PATH)
    init_db(con)

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda m: json.loads(m.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="satellite-consumer-group"
    )

    print(f"Consumer en écoute sur le topic '{TOPIC}'...")

    for message in consumer:
        pos = message.value
        try:
            con.execute("""
                INSERT INTO satellite_positions VALUES (?, ?, ?, ?, ?, ?)
            """, [
                pos["name"],
                pos["norad_id"],
                pos["latitude"],
                pos["longitude"],
                pos["altitude_km"],
                datetime.fromisoformat(pos["timestamp"])
            ])
            print(f"[{pos['timestamp']}] {pos['name']} → lat:{pos['latitude']} lon:{pos['longitude']} alt:{pos['altitude_km']}km")
        except Exception as e:
            print(f"Erreur insertion : {e}")

if __name__ == "__main__":
    run()