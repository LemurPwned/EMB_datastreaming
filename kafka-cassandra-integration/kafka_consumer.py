import avro.schema
import io
import avro.io 
from kafka import KafkaConsumer
from cassandra_query_system import QueryEngine
import cassandra

topic = 'emb'

consumer = KafkaConsumer(topic)

qe = QueryEngine(keyspace=topic)
schema = avro.schema.Parse(open("emb_schema.avsc").read())

for msg in consumer:
    try:
        qe.store_deserialized_emb_data(msg.value, schema)
    except cassandra.WriteTimeout:
        print("Timeout occurred...")
        pass
        
