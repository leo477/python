from random import randint,randrange
from kafka import KafkaProducer
import time
import json
import uuid

def key_serializer_function(k: str) -> bytes:
    return k.encode()


def value_serializer_function(v: dict) -> bytes:
    return json.dumps(v).encode("UTF-8")



def producer_1():
    kafka_producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=str.encode,
        value_serializer=value_serializer_function
    )

    topic_name = 'Homework14'
    while True:
        transaction_id: str = str(uuid.uuid4())
        data = {
            "id" : randint(100, 1000000) / 100,
            "from_account_id": randrange(1, 10)
        }
        kafka_producer.send(topic_name, key=transaction_id, value=data)
        print(f'Sent: {topic_name} - {transaction_id} - {data}')
        time.sleep(1)  # Send data every second

if __name__=='__main__':
    producer_1()