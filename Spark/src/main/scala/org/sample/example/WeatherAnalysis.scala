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

    // Filter out records where county or province is empty
    val filtered = weatherDF.filter("county != ''").filter("state_province != ''")

    // Filter out records where precipitation is non numeric
    val numeric = filtered.withColumn("hourlyprecip", $"hourlyprecip".cast("double")).na.drop(Seq("hourlyprecip"))

    // Group records by county and province and calculate the sum of precipitation
    val grouped = numeric.groupBy($"county", $"state_province").agg(sum("hourlyprecip"))

    // Rename the aggregate column to totalPrecipitation
    val groupedRenamed = grouped.withColumn("totalPrecipitation", $"sum(hourlyprecip)")

    // Create Data Frame with the population data
    val population = hiveContext.sql("select city, state, estimated_population from sample.projected_population")

    // Join the data using city and state
    val joinData = population.join(groupedRenamed, population("state") === groupedRenamed("state_province")
      && population("city") === groupedRenamed("county"), "inner")

    // Calculate the product of population and precipitation
    val wetPopulation = joinData.withColumn("wetPopulation", $"totalPrecipitation" * $"estimated_population")

    // Rename column to meaningful name
    val finalData = wetPopulation.drop($"county").drop($"state_province").drop($"sum(hourlyprecip)")

    // Sort data on population wetness
    val sortedData = finalData.sort($"wetPopulation".desc)

    // Display the results
    sortedData.write.saveAsTable("sample.msaPopulationWetness")
    sc.stop()
  }
}
