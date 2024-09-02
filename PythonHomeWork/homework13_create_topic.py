from kafka import KafkaAdminClient
from kafka.admin import NewTopic
import json

def create_topic(admin_client: KafkaAdminClient, topic_name: str):
    topics = [
        NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
    ]
    print(admin_client.create_topics(topics, validate_only=False))

def describe_topics(admin_client: KafkaAdminClient, topic_names: list[str]):
    print(f"Describing topics: {topic_names}")
    describe_topics_response = admin_client.describe_topics(topics=topic_names)
    print(json.dumps(describe_topics_response, indent=2))
    print("\n\n")


if __name__ == '__main__':
    ac = KafkaAdminClient(bootstrap_servers=["localhost:9092"])
    create_topic(ac, 'myfirstkafkaTopic')
    describe_topics(ac, ["myfirstkafkaTopic"])
