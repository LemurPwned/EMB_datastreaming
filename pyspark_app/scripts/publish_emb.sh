#!/bin/bash
set -x

for file in /scripts/EMB3_dataset_2017/*.csv;
do
  echo "New $file to stream"
  python producer_kafka.py $file 'emb' 'emb3'
  
done



