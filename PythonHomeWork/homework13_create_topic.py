from kafka import KafkaAdminClient
from kafka.admin import NewTopic

def create_topic(admin_client: KafkaAdminClient, topic_name: str):
    topics = [
        NewTopic(name=topic_name, num_partitions=3, replication_factor=1)
    ]


if __name__ == '__main__':
    ac = KafkaAdminClient(bootstrap_servers=["localhost:9092"])
    create_topic(ac, 'myfirstkafkaTopic')