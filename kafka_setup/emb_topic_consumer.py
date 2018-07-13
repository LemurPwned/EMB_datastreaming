from kafka import KafkaConsumer 

consumer = KafkaConsumer('emb')

anomaly_values = ['0', '0.00', '(na)']
for msg in consumer:
	print(msg)
	# process the message 
	# extract the anomaly
