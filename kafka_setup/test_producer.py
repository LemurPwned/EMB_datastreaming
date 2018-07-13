from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')
print(producer)
if producer is None:
 	quit()
for _ in range(100):
	future = producer.send('emb', b'mesg')
	future.get(timeout=20) 
