package part4advanced
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ProcessingTimeWindows {
  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Processing Time Window")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  def aggregateByProcessingTime() = {
    val linesCharCountByWindowDF = spark.readStream
      .format("socket")
      .option("host","127.0.0.1")
      .option("port",1234)
      .load()
      .select(col("value"),current_timestamp().as("processingTime")) // this is how you add processing time to a record
      .groupBy(window(col("processingTime"),"10 seconds").as("window"))
      .agg(sum(length(col("value"))).as("charCount"))
      .select(
        col("window").getField("start").as("start"),
        col("window").getField("end").as("end"),
        col("charCount")
      )

    linesCharCountByWindowDF.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }
  def main(args:Array[String]):Unit ={
    aggregateByProcessingTime()
  }
}
