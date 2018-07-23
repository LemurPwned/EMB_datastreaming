from kafka import KafkaProducer
import os
import time 



anomaly_vals = [b'(na)', b'0.00', b'0']
def emb_line_processing(line):
    parts = line.split(b',')
    flag = b'N,'
    for value in parts[3:]:
        if value in anomaly_vals:
            flag = b'A,'
    flag += line
    return flag

producer = KafkaProducer(bootstrap_servers='localhost:9092')
directory = '../sensordata/EMB3_dataset_2017'
files = os.listdir(directory)

for filename in files:
    if not filename.endswith('.csv'):
        continue
    with open(os.path.join(directory, filename), 'rb') as f:
        for line in f:
            future = producer.send('emb', emb_line_processing(line))
            future.get(timeout=10)
            time.sleep(1)
