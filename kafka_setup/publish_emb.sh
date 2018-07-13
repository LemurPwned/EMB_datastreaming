#!/bin/bash
set -x

for file in ../pyspark_app/scripts/EMB3_dataset_2017/*.csv;
do
  echo "New $file to stream"
  python36 emb_sensor_producer.py $file 'emb' 'emb3'
  
done



