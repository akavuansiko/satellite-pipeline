import time
import json
from datetime import datetime, timezone
from kafka import KafkaProducer
from skyfield.api import load, EarthSatellite
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")
TOPIC = "satellite_positions"
INTERVAL_SECONDS = 10

# TLE hardcodés (même source que fetch_tle.py)
HARDCODED_TLES = [
    {
        "name": "ISS (ZARYA)",
        "line1": "1 25544U 98067A   24170.54791667  .00018151  00000+0  32673-3 0  9990",
        "line2": "2 25544  51.6404 182.7015 0002829 198.0547 162.0395 15.50083731458380"
    },
    {
        "name": "TIANHE (CSS)",
        "line1": "1 48274U 21035A   24170.54791667  .00015000  00000+0  27000-3 0  9991",
        "line2": "2 48274  41.4750  95.0000 0003000  60.0000 300.0000 15.60000000000001"
    },
    {
        "name": "STARLINK-1",
        "line1": "1 43013U 17073A   24170.54791667  .00001000  00000+0  10000-4 0  9992",
        "line2": "2 43013  53.0500 200.0000 0001000  90.0000 100.0000 15.06000000000001"
    },
    {
        "name": "COSMOS 2251 DEB",
        "line1": "1 22675U 93036A   24170.54791667  .00003000  00000+0  30000-4 0  9993",
        "line2": "2 22675  74.0000 300.0000 0120000  45.0000 180.0000 14.80000000000001"
    },
    {
        "name": "IRIDIUM 33 DEB",
        "line1": "1 22677U 93036C   24170.54791667  .00002500  00000+0  25000-4 0  9994",
        "line2": "2 22677  86.4000 150.0000 0080000  30.0000  90.0000 14.90000000000001"
    },
]

def build_satellites():
    ts = load.timescale()
    satellites = []
    for tle in HARDCODED_TLES:
        sat = EarthSatellite(tle["line1"], tle["line2"], tle["name"], ts)
        satellites.append((sat, ts))
    return satellites

def compute_positions(satellites):
    positions = []
    for sat, ts in satellites:
        try:
            t = ts.now()
            geocentric = sat.at(t)
            subpoint = geocentric.subpoint()
            positions.append({
                "name":       sat.name,
                "norad_id":   sat.model.satnum,
                "latitude":   round(subpoint.latitude.degrees, 4),
                "longitude":  round(subpoint.longitude.degrees, 4),
                "altitude_km": round(subpoint.elevation.km, 2),
                "timestamp":  datetime.now(timezone.utc).isoformat(),
            })
        except Exception as e:
            print(f"Erreur position {sat.name}: {e}")
    return positions

def run():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )

    satellites = build_satellites()
    print(f"{len(satellites)} satellites chargés. Streaming toutes les {INTERVAL_SECONDS}s...")

    while True:
        positions = compute_positions(satellites)
        for pos in positions:
            producer.send(TOPIC, value=pos)
        producer.flush()
        print(f"[{datetime.now().strftime('%H:%M:%S')}] {len(positions)} positions envoyées sur Kafka")
        time.sleep(INTERVAL_SECONDS)

if __name__ == "__main__":
    run()