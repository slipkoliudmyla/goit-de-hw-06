import argparse
import json
import os
import random
import time
import uuid

from kafka import KafkaProducer

from configs import BOOTSTRAP_SERVERS, SENSORS_TOPIC


def main():
    parser = argparse.ArgumentParser(description="One process represents one sensor")
    parser.add_argument("--sensor-id", default=str(uuid.uuid4()))
    parser.add_argument("--interval", type=float, default=2)
    parser.add_argument("--count", type=int, default=0, help="0 means run until Ctrl+C")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()
    if args.interval < 0 or args.count < 0:
        parser.error("interval and count must be non-negative")
    rng = random.Random(args.seed)
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        acks="all",
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda key: key.encode("utf-8"),
    )
    print(f"START pid={os.getpid()} sensor={args.sensor_id} topic={SENSORS_TOPIC}", flush=True)
    sent = 0
    try:
        while args.count == 0 or sent < args.count:
            data = {
                "id": args.sensor_id,
                "temperature": round(rng.uniform(25, 45), 2),
                "humidity": round(rng.uniform(15, 85), 2),
                "timestamp": time.time(),
            }
            metadata = producer.send(SENSORS_TOPIC, key=args.sensor_id, value=data).get(timeout=15)
            print(json.dumps({"event": "SENT", "topic": metadata.topic,
                              "partition": metadata.partition, "offset": metadata.offset,
                              "data": data}), flush=True)
            sent += 1
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("Sensor stopped", flush=True)
    finally:
        producer.close()


if __name__ == "__main__":
    main()
