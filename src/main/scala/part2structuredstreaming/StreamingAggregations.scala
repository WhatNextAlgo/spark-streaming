package part2structuredstreaming

import org.apache.spark.sql.{Column,DataFrame,SparkSession}
import org.apache.spark.sql.functions._

object StreamingAggregations {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Streaming Aggregations")
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  def streamingCount() = {
    val lines:DataFrame = spark.readStream
      .format("socket")
      .option("host","127.0.0.1")
      .option("port",1234)
      .load()

    val lineCount:DataFrame = lines.selectExpr( "count(*) as lineCount")

    // aggregations with distinct are not supported
    // otherwise Spark will need to keep track of EVERYTHING

    lineCount.writeStream
      .format("console")
      .outputMode("complete") // append and update not supported on aggregations without watermark
      .start()
      .awaitTermination()
  }

  def numericalAggregations():Unit = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 1234)
      .load()

    // aggregate here
    val numbers = lines.select(col("value").cast("integer").as("number"))
    val aggregationDF = numbers.groupBy("number").count()

    aggregationDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def groupNames() ={
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 1234)
      .load()

    // counting occurrences of the "name" value
    val names = lines
      .select(col("value").as("name"))
      .groupBy(col("name"))
      .count()

    names.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
  def main(args:Array[String]):Unit = {
//    streamingCount()
//    numericalAggregations()
    groupNames()
  }

}
