from kafka import KafkaConsumer 

consumer = KafkaConsumer('emb')

for msg in consumer:
	print(msg)
