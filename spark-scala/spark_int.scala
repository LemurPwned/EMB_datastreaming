package cassandra_job

import org.apache.commons.math3.distribution.TDistribution
//import breeze.stats.distributions.StudentsT
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.functions._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import com.datastax.spark.connector._
import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector.cql.CassandraConnector 
import scala.collection.mutable.ListBuffer

import java.util.Calendar

import com.github.servicenow.ds.stats.stl.SeasonalTrendLoess




object CassandraInteg {

  def main(args: Array[String]): Unit = {
    val logFile = "/opt/spark/README.md"
    val spark = SparkSession.builder().appName("CassandraInteg")
      .config("spark.cassandra.connection.host", "localhost")
      .config("spart.cassandra.connection.port", "9042")
      //.setJars(Seq(System.getProperty("user.dir") + 
      //  "/target/scala-2.11/cassandrainteg_2.11-1.0.jar"))
      .getOrCreate();
    spark.sparkContext.setLogLevel("ERROR")
    println(s"Now database is printed out\n");
    
    val connector = CassandraConnector(spark.sparkContext.getConf)
    //CassandraConnector conn = CassandraConnector.apply(spark.sparkContext().conf())
    connector.withSessionDo(session =>
        session.execute("CREATE KEYSPACE IF NOT EXISTS anomal " +
                         "WITH REPLICATION = {'class': 'SimpleStrategy', " +
                                              "'replication_factor': 1}"))
    connector.withSessionDo(session =>
        session.execute("DROP TABLE IF EXISTS anomal.anomaly_data"))
    connector.withSessionDo(session =>
        session.execute("CREATE TABLE IF NOT EXISTS anomal.anomaly_data( " +
                          "timestamp timestamp, " +
                          "anomaly float, " +
                          "PRIMARY KEY (timestamp));")
    )
    
    val rdd = spark.sparkContext.cassandraTable("emb", "emb_data");
    //rdd.foreach(println);
    //println("Finished printing");
    
    val tmpSeries = spark.read.format("csv").option("header", "true").load("emb.csv")
    val tmSeries1 = tmpSeries.select("timestamp", "v1")
    val tmSeries = tmSeries1.withColumn("v1", tmSeries1("v1").cast(DoubleType)).na.fill(0.0, Seq("v1"))
    //println(tmSeries.printSchema)
    val anomalies = anomalyDetection(tmSeries, 21814, 30, 10000)
    val collection = spark.sparkContext.parallelize(anomalies)
    collection.saveToCassandra("anomal", "anomaly_data", SomeColumns("timestamp", "anomaly"))
    spark.stop()
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
                      List[(String, Double)] = {
    var anomalyList = new ListBuffer[(String, Double)]()

    // at least 2 periods are needed
    if (numberObs < numberObsPerPeriod*2) throw new IllegalStateException("2 periods needed." + 
                                                              "Insufficient number of observations")
    val data = dataset.select("v1").rdd.map(r=>r(0)).collect().map(_.asInstanceOf[Double])
    val dataTm = dataset.select("timestamp").rdd.map(r=>r(0)).collect().map(_.asInstanceOf[String])
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
    
    val R_idx: Array[(String, Double)] = new Array[(String, Double)](anomalyUpperBound)
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
              println(insertTuple)
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



