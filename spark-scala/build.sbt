name := "CassandraInteg"

version := "1.0"

scalaVersion := "2.11.8"

resolvers += "Spark Packages Repo" at "https://dl.bintray.com/spark-packages/maven"
libraryDependencies ++= Seq(
			"org.apache.spark" %% "spark-sql" % "2.3.1",
			"datastax" % "spark-cassandra-connector" % "2.3.1-s_2.11",
			"com.github.servicenow.stl4j" % "stl-decomp-4j" % "1.0.3",
			"org.apache.commons" % "commons-math3" % "3.2",
			"org.scalanlp" %% "breeze" % "0.13.2",
			"org.scalanlp" %% "breeze-natives" % "0.13.2",
			"org.scalanlp" %% "breeze-viz" % "0.13.2")

mergeStrategy in assembly := {
	case PathList("META-INF", xs @ _*) => MergeStrategy.discard
	case x => MergeStrategy.first
}

