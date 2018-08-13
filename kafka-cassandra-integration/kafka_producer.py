from kafka import KafkaProducer
import avro.io
import io
import avro.schema
import os
import time 
import sys

anomaly_vals = [b'(na)', b'0.00', b'0']
def emb_line_processing(line):
    parts = line.split(b',')
    flag = b'N,'
    for value in parts[3:]:
        if value in anomaly_vals:
            flag = b'A,'
    flag += line
    return flag

non_binary_vals = ['(na)', '0.00', '0']
def msg_processing(msg):
    msg[-1] = msg[-1].replace('\r\n', '')
    correct_vals = []
    flag = 'N'
    for value in msg[2:]:
        if value in non_binary_vals:
            flag = 'A'
            correct_vals.append(0.00)
        else:
            correct_vals.append(float(value))
    assert len(correct_vals) == len(msg[2:])
    return {"flag": flag, "timestamp": msg[0] + " " + msg[1] + 'Z',
            "v1": correct_vals[0], "v2": correct_vals[1], "v3": correct_vals[2], 
            "v4": correct_vals[3], "v5": correct_vals[4]}

schema = avro.schema.Parse(open("emb_schema.avsc").read())
spark_schema = avro.schema.Parse(open("cassandra_schema.avsc").read())

writer = avro.io.DatumWriter(schema)
spark_writer = avro.io.DatumWriter(spark_schema)
# this is universal

producer = KafkaProducer(bootstrap_servers='localhost:9092')
directory = '../sensordata/EMB3_dataset_2017'
files = os.listdir(directory)

period =  24 #1 day, is expressed in terms of lowest granularity ie. hours/days
obs = 5*period # number of observations i.e how many periods to take
tstart = -1
tstop = -1
agg_count = 0

for filename in files:
    if not filename.endswith('.csv'):
        continue
    with open(os.path.join(directory, filename), 'rb') as f:
        for line in f:                
            msg = line.decode('utf-8').split(',')
            to_send = msg_processing(msg)
            if agg_count == 0:
                # this is a first timestamp for spark
                tstart = to_send['timestamp']
            bytes_writer = io.BytesIO()
            encoder = avro.io.BinaryEncoder(bytes_writer)
            writer.write(to_send, encoder)
            raw_bytes = bytes_writer.getvalue()
            future = producer.send('emb', raw_bytes)
            future.get(timeout=10)
            time.sleep(0.5)

            agg_count += 1
            if (agg_count == obs): 
                time.sleep(10) # wait for messages to be logged in Cassandra
                # this is a second timestamp for spark
                tstop = to_send['timestamp'] 
                bytes_writer = io.BytesIO()
                encoder = avro.io.BinaryEncoder(bytes_writer)
                spark_writer.write({"timestamp_start": tstart, 
                              "timestamp_stop": tstop,
                              "period": period,
                              "observations": obs}, encoder)
                raw_bytes = bytes_writer.getvalue()
                future = producer.send('spark_emb', raw_bytes)
                future.get(timeout=10)
                print(tstart, tstop, period, obs)
                # zero out the msg
                agg_count = 0
                tstart = -1
                tstop = -1
                print("Sent to spark")
		
