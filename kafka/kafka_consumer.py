from kafka import KafkaConsumer

consumer = KafkaConsumer('log-result')

for msg in consumer:
    print(msg)
