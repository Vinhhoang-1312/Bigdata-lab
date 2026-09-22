#!/usr/bin/env python3
"""Continuous live trip streamer for UrbanFlow.

Streams realistic NYC TLC taxi trips to Kafka (urbanflow.trip-events) every second.
Simulated event time advances continuously, driving real-time 5-minute tumbling
window aggregations and live Gold metric updates in Spark Streaming.
"""

from __future__ import annotations

import datetime
import json
import random
import sys
import time

try:
    from kafka import KafkaProducer
except ImportError:
    print("Error: kafka-python-ng is required. Install via pip install kafka-python-ng.")
    sys.exit(1)

TOPIC = "urbanflow.trip-events"
BOOTSTRAP_SERVERS = "localhost:9094"

POPULAR_ZONES = [
    (161, "Midtown Center"),
    (236, "Upper East Side North"),
    (237, "Upper East Side South"),
    (230, "Times Square/Theatre District"),
    (186, "Penn Station/Madison Sq West"),
    (132, "JFK Airport"),
    (138, "LaGuardia Airport"),
    (79, "East Village"),
    (48, "Clinton East"),
    (142, "Lincoln Square East"),
]

DROPOFF_ZONES = [48, 79, 132, 138, 142, 161, 170, 186, 230, 234, 236, 237, 239]


def run_streamer(rate_per_sec: int = 5, event_speed_multiplier: int = 15) -> None:
    print(f"Connecting to Kafka broker at {BOOTSTRAP_SERVERS}...")
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: str(k).encode("utf-8"),
        value_serializer=lambda v: json.dumps(v, separators=(",", ":")).encode("utf-8"),
        acks=1,
    )
    print(f"Connected! Streaming to topic '{TOPIC}' at ~{rate_per_sec} trips/sec...")

    # Start from Monday 22:15 PM New York time (continuing from current live state)
    simulated_time = datetime.datetime(2025, 1, 6, 22, 15, 0, tzinfo=datetime.timezone(datetime.timedelta(hours=-5)))
    total_sent = 0
    surge_zone_idx = 0

    try:
        while True:
            # Advance simulated time
            simulated_time += datetime.timedelta(seconds=event_speed_multiplier)
            time_str = simulated_time.isoformat()

            # Randomly pick a surge zone to push high demand
            if random.random() < 0.2:
                surge_zone_idx = (surge_zone_idx + 1) % len(POPULAR_ZONES)

            batch_count = random.randint(max(1, rate_per_sec - 2), rate_per_sec + 2)

            for _ in range(batch_count):
                total_sent += 1
                # 60% chance to pick the surge zone to create visible spikes
                if random.random() < 0.6:
                    pickup_zone = POPULAR_ZONES[surge_zone_idx][0]
                else:
                    pickup_zone = random.choice(POPULAR_ZONES)[0]

                dropoff_zone = random.choice(DROPOFF_ZONES)
                distance = round(random.uniform(0.8, 8.5), 2)
                duration = round(distance * random.uniform(3.0, 6.0), 2)
                fare = round(5.0 + distance * 2.8 + random.uniform(0, 3), 2)

                event = {
                    "event_id": f"trip-{simulated_time.strftime('%Y%m%d%H%M%S')}-{pickup_zone}-{total_sent:06d}",
                    "event_time": time_str,
                    "pickup_zone_id": pickup_zone,
                    "dropoff_zone_id": dropoff_zone,
                    "trip_distance": distance,
                    "trip_duration_minutes": duration,
                    "fare_amount": fare,
                }

                producer.send(TOPIC, key=pickup_zone, value=event)

            producer.flush()
            surge_name = POPULAR_ZONES[surge_zone_idx][1]
            print(
                f"[Live Streamer] Sent {batch_count:2d} trips | "
                f"Simulated Clock (NY): {simulated_time.strftime('%H:%M:%S')} | "
                f"Active Surge: {surge_name} | "
                f"Total Sent: {total_sent}",
                flush=True,
            )
            time.sleep(1.0)

    except KeyboardInterrupt:
        print("\nStopping streamer...")
    finally:
        producer.close()
        print("Kafka producer closed.")


if __name__ == "__main__":
    run_streamer()
