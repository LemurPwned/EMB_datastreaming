import sys
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers='localhost:9092')

data = sys.argv[1]
topic = sys.argv[2]

with open(data, rb) as f:
	for line in f:
		future = producer.send(topic, line)
		future.get(timeout=20) 
		sleep(5)
