#!/bin/bash
set -x 


#./create-topic.sh kafka zookeeper:2181 emb

docker exec -i $1 /opt/kafka_2.11-1.1.0/bin/kafka-topics.sh --create --zookeeper $2 --replication-factor 1 --partitions 1 --topic $3


