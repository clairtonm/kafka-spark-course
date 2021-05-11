# Exercício
# Enviar tweets para tópico twitter com a key covid

producer = KafkaProducer(value_serializer=lambda v: json.dumps(v).encode('utf-8'))