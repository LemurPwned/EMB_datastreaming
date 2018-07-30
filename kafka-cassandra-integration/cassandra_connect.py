from cassandra.cluster import Cluster
import cassandra

# change below to an array of addresses in order to connect to other IP addresses
cluster = Cluster()
session = cluster.connect()

keyspace_init = """
CREATE KEYSPACE emb
  WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 } """
table_init = """
CREATE TABLE IF NOT EXISTS emb.emb_data
(       
        id int,
        flag ascii,
        timestamp timestamp,
        v1 float,
        v2 float,
        v3 float,
        v4 float,
        v5 float,
        PRIMARY KEY (id, timestamp)
) WITH CLUSTERING ORDER BY (timestamp ASC);
"""
drop = """
    DROP TABLE IF EXISTS emb.emb_data;
"""
session.execute(drop) # for testing
try:
    session.execute(keyspace_init)
except cassandra.AlreadyExists:
    pass
session.execute(table_init)

