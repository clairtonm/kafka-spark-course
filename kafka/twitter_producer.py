from kafka import KafkaProducer
import json
import time
from kafka.errors import KafkaTimeoutError


with open("../data/tweets.json") as jsonFile:
    data = json.load(jsonFile)

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))
try:
    for tweet in data:
        producer.send('twitter', key=b'covid', value=tweet)
        print('Sent:', tweet)
        time.sleep(0.1)
except KafkaTimeoutError:
    print("Timeout: not possible to send the data.")
finally:
    producer.close()

