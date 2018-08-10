package cassandra_job

import org.apache.commons.math3.distribution.TDistribution
import org.apache.spark.SparkFiles
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector 
import scala.collection.mutable.ListBuffer

import com.github.servicenow.ds.stats.stl.SeasonalTrendLoess

import java.sql.Timestamp
import java.util.Properties
import java.util.Collection
import scala.collection.JavaConverters._

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.TimeoutException
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}


import org.apache.log4j.{LogManager, Level}

import org.apache.avro.io.{Decoder, DecoderFactory, DatumReader}
import org.apache.avro.specific.SpecificDatumReader
import org.apache.avro.generic.GenericRecord
import org.apache.avro.Schema


import scala.io.Source
import anomalyStruct.AnomalyData

object CassandraInteg {
  // logger
  val log = LogManager.getRootLogger

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("CassandraInteg")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spark.cassandra.connection.port", "9042")
      .config("spark.cassandra.connection.connections_per_executor_max", "3")
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    log.setLevel(Level.WARN)
    //val connector = CassandraConnector(spark.sparkContext.getConf)
    //prepareDatabase(connector)
    kafkaConsumer(spark)
    //val tm = spark.read.format("csv").option("header", "true").load("emb.csv")
    //runADJob(spark, tm, 21814. 30, 10000) 
    spark.stop()
  }

  def runADJob(spark: SparkSession,
               dataframe: DataFrame,
               numObs: Int,
               period: Int,
               anomalies: Int): Unit ={
    log.warn("Running an anomaly detection job")
    val tmSeries1 = dataframe.select("timestamp", "v1")
    val tmSeries = tmSeries1.withColumn("v1", tmSeries1("v1").cast(DoubleType)).na.fill(0.0, Seq("v1"))
    val anomaliesData = anomalyDetection(tmSeries, numObs, period, anomalies)
    log.warn("Saving anomaly data to Cassandra...")
    if (anomaliesData.size != 0){
       val collection = spark.sparkContext.parallelize(anomaliesData)
       collection.saveToCassandra("anomal", "anomaly_data", SomeColumns("timestamp", "anomaly"))
    }
    log.warn("Finished Anomaly Detection Job...")
  }

  def prepareDatabase(connector: CassandraConnector):Unit ={
    log.warn("Prepraing anomaly database...")  
    connector.withSessionDo(session =>
        session.execute("""
                    CREATE KEYSPACE IF NOT EXISTS anomal 
                    WITH REPLICATION = {'class': 'SimpleStrategy', 
                                        'replication_factor': 1};
                    """
                    ))
     connector.withSessionDo(session =>
       session.execute("DROP TABLE IF EXISTS anomal.anomaly_data"))
     connector.withSessionDo(session =>
       session.execute("""
                      CREATE TABLE IF NOT EXISTS anomal.anomaly_data( 
                      timestamp timestamp, 
                      anomaly float, 
                      PRIMARY KEY (timestamp));
                     """
                     ))
    // register the view for spark
  }                   

  def kafkaConsumer(spark: SparkSession): Unit = {
    val properties = new Properties()
    properties.put("bootstrap.servers", "localhost:9092")
    properties.put("group.id", "consumer")
    properties.put("key.deserializer", classOf[StringDeserializer])
    properties.put("value.deserializer", classOf[ByteArrayDeserializer])
    properties.put("auto.commit.interval.ms","8000")

    val kafkaConsumer = new KafkaConsumer[String, Array[Byte]](properties)
    kafkaConsumer.subscribe(Seq("spark_emb").asJava)
    log.warn("Boostrapping kafka consumption...")
    
    val filePath = SparkFiles.get("cassandra_schema.avsc")
    val schemaStr = Source.fromFile(filePath).mkString
    val schema = new Schema.Parser().parse(schemaStr)

    log.warn("SCHEMA PATH " + filePath)
    while (true) {
      val results = kafkaConsumer.poll(2000).asScala
      for (record <- results){
        try{ 
          val data = parseData(record.value(), schema)
          val df = spark.read.format("org.apache.spark.sql.cassandra")
                             .options(Map("table"->"emb_data", "keyspace"->"emb"))
                             .load()
                             .filter(s"timestamp >= '${data.timestampStart}' AND" 
                               +s" timestamp <= '${data.timestampStop}'")
          log.warn(data.toString)
          log.warn(df.rdd.isEmpty)
          if (!df.rdd.isEmpty()){
            log.warn("Processing for: " + data.toString)
            val anomalUpperBound = 10 // Hardcoded, idk what it should be
            runADJob(spark, df, data.observations, data.period, anomalUpperBound)
          }
          kafkaConsumer.commitSync
        }
        catch {
          case timeOutEx: TimeoutException => println("Timeout")
        }
      }
    }
  }

  def parseData(msg: Array[Byte], schema: Schema): AnomalyData = {
    val reader = new SpecificDatumReader[GenericRecord](schema)
    val decoder = DecoderFactory.get().binaryDecoder(msg, null)
    val data = reader.read(null, decoder)
    new AnomalyData(data.get("timestamp_start").toString,
                    data.get("timestamp_stop").toString,
                    data.get("period").asInstanceOf[Int],
                    data.get("observations").asInstanceOf[Int])
  }

  /**
   * Performs Anomaly Detection on a timeseries
   * @dataset: Rdd of timeseries
     * @numberObs: number of observations i.e. length of Rdd
   * @numberObsPerPeriod: number observations in a given period, used during stl
   * @anomalyUpperBound: maximum number of expected anomalies, must be less
   *                     than 49% of observations
   * @significanceLevel: significance level for statistical test
   *
   */
  def anomalyDetection(dataset: DataFrame,
                       numberObs: Int,
                       numberObsPerPeriod: Int,
                       anomalyUpperBound: Int,
                       significanceLevel: Double = 0.05): 
                      List[(Timestamp, Double)] = {
    var anomalyList = new ListBuffer[(Timestamp, Double)]()

    // at least 2 periods are needed
    if (numberObs < numberObsPerPeriod*2) throw new IllegalStateException("2 periods needed." + 
                                                              "Insufficient number of observations")
    val data = dataset.select("v1").rdd.map(r=>r(0)).collect().map(_.asInstanceOf[Double])
    val dataTm = dataset.select("timestamp").rdd.map(r=>r(0)).collect().map(_.asInstanceOf[Timestamp])
   
    println(data.size, dataTm.size, numberObs)
    try{
	assert(data.size == dataTm.size)
	assert(data.size == numberObs)
    }
    catch {
	case assertEx: AssertionError => println("Size mismatch in Cassandra database")
    }

    val stlBuilder = new SeasonalTrendLoess.Builder
    val stlParam = stlBuilder
                   .setPeriodLength(numberObsPerPeriod)
                   .setPeriodic()
                   .setRobust()
                   .buildSmoother(data)
    val stl = stlParam.decompose()
    val seasonal = stl.getSeasonal()
    val trend = stl.getTrend()
    
    //remove seasonal component from data and median to create univariate remainder
    val med = dataset.stat.approxQuantile("v1", Array(0.5), 0.001)(0) // get median
     
    var univariateComponent = data.zip(seasonal).map(x => x._1 - x._2 - med)
    val smoothedSeasonal = data.zip(trend).map(x => x._1 + x._2)
    //smoothedSeasonal.foreach(println)   
    
    var numAnom = 0
    for (i <- 1 to anomalyUpperBound){
            val med = median(univariateComponent)
            var residual = univariateComponent.map(x => Math.abs(x - med))
            val mdev = medianAbsoluteDeviation(univariateComponent)*1.4826
            if (mdev != 0){
              residual = residual.map(x => x/mdev)
              val maxRes = residual.reduceLeft(_ max _)
              val idMax = residual.indexOf(maxRes)
              // mistake here -- changing size of an array
              var insertTuple = (dataTm(idMax), data(idMax))
              anomalyList += insertTuple
              // remove the anomaly from the dataset 
              univariateComponent = univariateComponent.take(idMax) ++ univariateComponent.drop(idMax+1)

              val pVal = 1 - significanceLevel/(2*(numberObs+1-i))
              // get t-student distribution and compare against anomalyLevel
              val t = new TDistribution(numberObs-1-i).inverseCumulativeProbability(pVal)
              val thres = t*(numberObs-i)/Math.sqrt((numberObs-i-1+Math.pow(t, 2))*(numberObs-i+1)) 
              if (maxRes > thres) numAnom = i
            }            
    }
    log.warn("Finished detecting anomalies")
    log.warn(s"DETECTED ANOMALIES $numAnom") 
    anomalyList.toList
 }

  def median(values: Array[Double]): Double = {
    val (low, up) = values.sortWith(_ < _).splitAt(values.size/2)
    if (values.size %2 == 0) (low.last + up.head)/2 else up.head
  }

  def medianAbsoluteDeviation(dataBuffer: Array[Double]): Double = {
    val t_median = median(dataBuffer)
    val absoluteMedian = dataBuffer.map(x => Math.abs(x - t_median))
    median(absoluteMedian)
  }
}



