from kafka.producer import KafkaProducer
from time import sleep
import json
import uuid

def value_serializer_function(v) -> bytes:
    return json.dumps(v).encode("UTF-8")

def producer_1():
    producer = KafkaProducer(
        bootstrap_servers='localhost:9092',
        key_serializer=str.encode,
        # value_serializer=lambda v: json.dumps(v).encode("UTF-8"),
        value_serializer=value_serializer_function
    )
    topic_name='myfirstkafkaTopic'
    i=0
    while True:
        i+=1
        current_key = str(uuid.uuid4())
        value = {
            "key_data": f"Message # {i}",
        }
        str_message="message # " + str(i)
        print(str_message)
        producer.send(topic_name, value=value, key=current_key)
        sleep(1)

if __name__=='__main__':
    producer_1()