import json
import os
import random
import sys
import time
from collections import deque
from datetime import datetime, timedelta

import pytz
from faker import Faker
from kafka import KafkaProducer

from src.utils.exception import TrafficPipelineException
from src.utils.logger import logger

from dotenv import load_dotenv
load_dotenv()


fake = Faker()
utc = pytz.utc


# Map roads to zones so road_id and city_zone stay logically consistent
roads = {
    "R100": "CBD",
    "R200": "AIRPORT",
    "R300": "TECHPARK",
    "R400": "SUBURB",
    "R500": "TRAINSTATION"
}

weather_options = ["CLEAR", "RAIN", "FOG", "STORM"]
vehicle_cache = deque(maxlen=5000)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic-topic")
MIN_SLEEP_SECONDS = float(os.getenv("MIN_SLEEP_SECONDS", "0.5"))
MAX_SLEEP_SECONDS = float(os.getenv("MAX_SLEEP_SECONDS", "1.5"))


# Rush-hour logic to create time-based traffic patterns
def is_rush_hour(hour):
    return (7 <= hour <= 10) or (17 <= hour <= 20)


# Weighted weather choice so all weather types are not equally likely
def choose_weather():
    return random.choices(
        population=weather_options,
        weights=[0.55, 0.20, 0.15, 0.10],
        k=1
    )[0]


# Congestion depends on zone, time, weather, and incidents
def compute_congestion(zone, weather, hour, incident_flag):
    score = 2.0

    if zone in ["CBD", "TECHPARK", "TRAINSTATION"] and is_rush_hour(hour):
        score += 1.8
    elif zone == "AIRPORT" and (6 <= hour <= 9 or 18 <= hour <= 22):
        score += 1.2
    elif zone == "SUBURB" and not is_rush_hour(hour):
        score -= 0.6

    if weather == "RAIN":
        score += 0.7
    elif weather == "FOG":
        score += 0.9
    elif weather == "STORM":
        score += 1.4

    if incident_flag:
        score += 1.5

    # Add some noise so the data is not too perfectly controlled
    score += random.uniform(-0.8, 0.8)

    return max(1, min(5, round(score)))


# Speed generally drops as congestion and bad weather increase
def compute_speed(zone, congestion_level, weather):
    base_speed = {
        "CBD": 45,
        "AIRPORT": 60,
        "TECHPARK": 50,
        "SUBURB": 70,
        "TRAINSTATION": 40
    }[zone]

    weather_penalty = {
        "CLEAR": 0,
        "RAIN": 8,
        "FOG": 12,
        "STORM": 18
    }[weather]

    congestion_penalty = (congestion_level - 1) * 10
    noise = random.randint(-6, 6)

    speed = base_speed - weather_penalty - congestion_penalty + noise

    return max(5, min(110, speed))


# Traffic volume is useful for analytics and later ML tasks
def compute_traffic_volume(zone, congestion_level, hour):
    base_volume = {
        "CBD": 120,
        "AIRPORT": 90,
        "TECHPARK": 110,
        "SUBURB": 70,
        "TRAINSTATION": 100
    }[zone]

    if is_rush_hour(hour):
        base_volume += 40

    base_volume += (congestion_level - 1) * 15
    base_volume += random.randint(-20, 20)

    return max(10, base_volume)


# Generate clean events with realistic relationships between features
def generate_clean_event():
    event_dt = datetime.now(utc)
    hour = event_dt.hour

    road_id = random.choice(list(roads.keys()))
    city_zone = roads[road_id]
    weather = choose_weather()

    # Small chance of a traffic incident
    incident_flag = 1 if random.random() < 0.06 else 0

    congestion_level = compute_congestion(city_zone, weather, hour, incident_flag)
    speed = compute_speed(city_zone, congestion_level, weather)
    traffic_volume = compute_traffic_volume(city_zone, congestion_level, hour)

    vid = fake.uuid4()
    vehicle_cache.append(vid)

    return {
        "vehicle_id": vid,
        "road_id": road_id,
        "city_zone": city_zone,
        "speed": speed,
        "congestion_level": congestion_level,
        "traffic_volume": traffic_volume,
        "incident_flag": incident_flag,
        "weather": weather,
        "event_time": event_dt.isoformat()
    }


# Generate intentionally bad records so downstream cleaning logic has work to do
def generate_dirty_event():
    dirty_type = random.choice([
        "null_speed",
        "negative_speed",
        "extreme_speed",
        "duplicate_vehicle",
        "late_event",
        "future_event",
        "wrong_datatype",
        "schema_drift",
        "corrupt_json"
    ])

    logger.warning(f"Generating dirty event of type: {dirty_type}")

    base = generate_clean_event()

    if dirty_type == "null_speed":
        base["speed"] = None

    elif dirty_type == "negative_speed":
        base["speed"] = -40

    elif dirty_type == "extreme_speed":
        base["speed"] = 420

    elif dirty_type == "duplicate_vehicle" and vehicle_cache:
        base["vehicle_id"] = random.choice(vehicle_cache)

    elif dirty_type == "late_event":
        base["event_time"] = (
            datetime.now(utc) - timedelta(minutes=random.randint(10, 120))
        ).isoformat()

    elif dirty_type == "future_event":
        base["event_time"] = (
            datetime.now(utc) + timedelta(minutes=random.randint(5, 60))
        ).isoformat()

    elif dirty_type == "wrong_datatype":
        base["speed"] = "FAST"

    elif dirty_type == "schema_drift":
        base["road_condition"] = random.choice(
            ["GOOD", "BAD", "UNDER_CONSTRUCTION"]
        )

    elif dirty_type == "corrupt_json":
        return b'{"vehicle_id": "broken-payload", "speed": '

    return base


def serialize_event(value):
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    return json.dumps(value).encode("utf-8")


def build_producer():
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=serialize_event,
        acks="all",
        retries=3
    )
    logger.info("Kafka producer initialized successfully")
    return producer


def send_event(producer, event):
    if isinstance(event, bytes):
        producer.send(KAFKA_TOPIC, value=event)
        logger.warning("Malformed payload sent to Kafka topic")
        print("MALFORMED EVENT SENT")
        return

    producer.send(KAFKA_TOPIC, value=event)
    print(event)


def main():
    event_count = 0
    producer = None

    try:
        producer = build_producer()

        while True:
            if random.random() < 0.75:
                event = generate_clean_event()
            else:
                event = generate_dirty_event()

            send_event(producer, event)

            if not isinstance(event, bytes):
                event_count += 1

                if event_count % 50 == 0:
                    logger.info(f"{event_count} events sent successfully")

            time.sleep(random.uniform(MIN_SLEEP_SECONDS, MAX_SLEEP_SECONDS))

    except KeyboardInterrupt:
        logger.info("Traffic producer stopped by user")
    except Exception as e:
        logger.error(str(TrafficPipelineException(e, sys)))
        raise TrafficPipelineException(e, sys) from e
    finally:
        if producer is not None:
            producer.flush()
            producer.close()


if __name__ == "__main__":
    main()
