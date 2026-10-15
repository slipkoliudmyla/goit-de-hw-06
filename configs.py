import os
from pathlib import Path

BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:19092").split(",")
PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "slipkoliudmyla_hw06")
SENSORS_TOPIC = f"{PREFIX}_building_sensors"
ALERTS_TOPIC = f"{PREFIX}_alerts"
CONDITIONS_FILE = Path(__file__).with_name("alerts_conditions.csv")
