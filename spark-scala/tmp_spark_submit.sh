/opt/spark/bin/spark-submit --class cassandra_job.CassandraInteg --master local --jars avro-1.8.2.jar,kafka-clients-1.1.1.jar,commons-math3-3.6.1.jar,stl-decomp-4j-1.0.4-SNAPSHOT.jar,datastax_cassandra_connector.jar target/scala-2.11/cassandrainteg_2.11-1.0.jar
