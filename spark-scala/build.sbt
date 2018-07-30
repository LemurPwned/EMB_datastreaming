name := "CassandraInteg"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies ++= Seq(
			"org.apache.spark" %% "spark-sql" % "2.3.1",
			"datastax" % "spark-cassandra-connector" % "2.3.1-s_2.11",
			"com.github.servicenow.stl4j" % "stl-decomp-4j" % "1.0.3",
			"org.apache.commons" % "commons-math3" % "3.2",
			"org.apache.kafka" %% "kafka" % "1.1.1",
			"org.apache.kafka" % "kafka-clients" % "1.1.1",
			"org.apache.avro" % "avro" % "1.8.2",
			"io.confluent" % "kafka-avro-serializer" % "3.2.1")
mergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}

