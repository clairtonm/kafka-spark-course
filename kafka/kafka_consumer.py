from kafka import KafkaConsumer

consumer = KafkaConsumer('log')

for msg in consumer:
    print(msg)
