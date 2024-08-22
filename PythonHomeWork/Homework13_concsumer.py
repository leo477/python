from kafka import KafkaConsumer
import time
import os

def post_to_file(messages):
    timestamp = int(time.time())
    filename = f"data_portion_{timestamp}.txt"

    with open(filename, 'w') as f:
        for message in messages:
            f.write(f"{message}\n")

def consumer_1():
    buffer = []
    topic_name = "myfirstkafkaTopic"
    consumer = KafkaConsumer(topic_name, bootstrap_servers=["localhost:9092"])
    for msg in consumer:
        print(msg)
        buffer.append(msg)

        if len(buffer) >= 50:
            post_to_file(buffer)
            buffer = []


if __name__ == '__main__':
    consumer_1()