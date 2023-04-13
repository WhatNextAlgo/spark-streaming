package part2structuredstreaming

import org.apache.spark.sql.{DataFrame,SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
object StreamingDataFrames {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Spark Structure Streaming")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def readFormStocket() ={
    // reading a DF

    val lines:DataFrame = spark.readStream
      .format("socket")
      .option("host","127.0.0.1")
      .option("port",1234)
      .load()

    val shortLines:DataFrame = lines.filter(length(col("value") < 5))

    // tell between s static vs a streaming DF
    println(shortLines.isStreaming)
    // consuming a DF
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    // wait for the stream to finish
    query.awaitTermination()
  }

  def readFromFiles() = {
    val stocksDF:DataFrame = spark.readStream
      .format("csv")
      .option("header","false")
      .option("dateFormat","MMM d yyyy")
      .schema(stocksSchema)
      .load("src/main/resources/data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 1234)
      .load()

    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        // Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query
        // Trigger.Once() // single batch, then terminate
        Trigger.Continuous(2.seconds) // experimental, every 2 seconds create a batch with whatever you have
      )
      .start()
      .awaitTermination()
  }

  def main(args:Array[String]):Unit  ={
    demoTriggers()
  }



}
