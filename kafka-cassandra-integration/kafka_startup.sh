cd /opt/kafka/

# start zookeeper
tmux split-window 'bin/zookeeper-server-start.sh config/zookeeper.properties'

sleep 15

# start kafka server
tmux split-window 'bin/kafka-server-start.sh config/server.properties'

sleep 15
# create topic
bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic emb
