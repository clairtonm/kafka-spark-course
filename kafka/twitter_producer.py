from kafka import KafkaProducer
import json

with open("../data/tweets.json") as jsonFile:
    data = json.load(jsonFile)

print(data)

producer = KafkaProducer()

future = producer.send('test-topic', b'Time do asses eh o melhor da Dell/Atlantico')