import argparse
import json

from kafka import KafkaConsumer

from configs import ALERTS_TOPIC, BOOTSTRAP_SERVERS, PREFIX


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--group", default=f"{PREFIX}_alert_reader")
    parser.add_argument("--idle-timeout", type=int, default=0)
    args = parser.parse_args()
    consumer = KafkaConsumer(
        ALERTS_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=args.group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        consumer_timeout_ms=args.idle_timeout * 1000 if args.idle_timeout else float("inf"),
        value_deserializer=lambda value: json.loads(value.decode("utf-8")),
    )
    try:
        for message in consumer:
            print(json.dumps({"event": "ALERT_RECEIVED", "topic": message.topic,
                              "partition": message.partition, "offset": message.offset,
                              "data": message.value}), flush=True)
            consumer.commit()
    except KeyboardInterrupt:
        print("Alert reader stopped", flush=True)
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
