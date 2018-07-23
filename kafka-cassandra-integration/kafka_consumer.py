from kafka import KafkaConsumer
from cassandra_query_system import QueryEngine

consumer = KafkaConsumer('emb')

qe = QueryEngine(keyspace='emb')

for msg in consumer:
    print(msg.value)
    qe.store_emb_data(msg.value)
