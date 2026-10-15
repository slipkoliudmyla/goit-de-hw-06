import time

from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

from configs import ALERTS_TOPIC, BOOTSTRAP_SERVERS, SENSORS_TOPIC


def main():
    admin_client = KafkaAdminClient(bootstrap_servers=BOOTSTRAP_SERVERS)
    required = {SENSORS_TOPIC, ALERTS_TOPIC}
    try:
        for name in sorted(required):
            try:
                admin_client.create_topics(
                    [NewTopic(name=name, num_partitions=2, replication_factor=1)]
                )
                print(f"CREATED {name}", flush=True)
            except TopicAlreadyExistsError:
                print(f"EXISTS {name}", flush=True)
        for _ in range(20):
            if required.issubset(admin_client.list_topics()):
                break
            time.sleep(0.5)
        else:
            raise RuntimeError("Created topics did not appear in Kafka metadata")
        for topic in sorted(admin_client.list_topics()):
            if topic in required:
                print(topic, flush=True)
    finally:
        admin_client.close()


if __name__ == "__main__":
    main()
