package org.sample.example

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Prateek Sheel on 4/15/17.
  * A simple Spark use case to join two data sets and write the output to HDFS
  */
object WeatherAnalysis {

  def main(args: Array[String]){
    // Create the spark configuration. Usually a master has to be specified (e.g. local or YARN).
    // In this case, it will be done while submitting the spark job.
    // The following can be used to set the master and run the code from within IntelliJ: .setMaster("local[*]")
    val sparkConf = new SparkConf().setMaster("local[*]").setAppName("Weather Analysis").set("spark.storage.memoryFraction", "1")

    // Create the spark context - it acts as a proxy to the Spark framework
    val sc = new SparkContext(sparkConf)

    // Enable recursive reading of files in directories
    sc.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive","true")
    val blockSize = 1024 * 1024 * 128 // 128 MB
    sc.hadoopConfiguration.setInt("dfs.blocksize", blockSize)

    // Create hive context using configuration
    val hiveContext = new HiveContext(sc)
    import hiveContext.implicits._
    import org.apache.spark.sql.functions._

    // HDFS Name Node and Hive MetaStore URL
    val hive_metastore_server = "quickstart.cloudera:9083"
    val namenode_server = "hdfs://quickstart.cloudera:8020"
    val dbname = "sample"

    // A hive table named projected_population exists and is stored in text format
    val inputPath = namenode_server + "/user/hive/warehouse/sample.db/projected_population/state=WY"
    val outputPath = namenode_server + "/user/hive/warehouse/sample.db/msa_output"

    /*** Place to read your data and perform transformations*/
    //val rdd = sc.textFile(inputPath).flatMap(x=>x.split(",")).map(word=>(word,1)).reduceByKey(_+_, 1)
    //rdd.saveAsTextFile(outputPath)

    // Create Data Frame with the precipitation for each county and province
    val weatherDF = hiveContext.sql("select county, state_province, hourlyprecip from sample.weather_data")
    val numeric = weatherDF.withColumn("hourlyprecip", $"hourlyprecip".cast("double")).na.drop(Seq("hourlyprecip"))
    //val mapData = numeric.flatMap(record => )
    //val mapData = numeric.map(record => Row(Seq(record).take(2).toList.mkString("_") , record(2)))
    //mapData.collect().foreach(println)
    val grouped = numeric.groupBy($"county", $"state_province").agg(sum("hourlyprecip"))
    //val grouped = numeric.groupBy($"county", $"state_province").agg($"county", $"state_province", sum("hourlyprecip"))
    grouped.show()
    sc.stop()
  }
}
