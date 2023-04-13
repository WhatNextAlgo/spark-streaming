package part2structuredstreaming

import org.apache.spark.sql.{DataFrame,Encoders,Dataset,SparkSession}
import org.apache.spark.sql.functions._
import common._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
object StreamingDatasets {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Spark Streaming Dataset")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  import spark.implicits._

  def readCars():Dataset[Car] = {
    // Useful for DF  -> DS transformations
    val carEncoder = Encoders.product[Car]

    spark.readStream
      .format("socket")
      .option("host","127.0.0.1")
      .option("port",1234)
      .load() // DF with single string column "value"
      .select(from_json(col("value"),carSchema).as("car")) //composite column (struct)
      .selectExpr("car.*")// DF with multiple columns
      .as[Car] // encoder can be passed implicitly with spark.implicits
  }

  def showCarNames() = {
    val carsDS:Dataset[Car] = readCars()

    // transformations here
    val carNamesDF:DataFrame = carsDS.select(col("Name")) // DF

    // collection transformations maintain type info
    val carNamesAlt:Dataset[String] = carsDS.map(_.Name)

    carNamesAlt.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  /**
   * Exercises
   *
   * 1) Count how many POWERFUL cars we have in the DS (HP > 140)
   * 2) Average HP for the entire dataset
   * (use the complete output mode)
   * 3) Count the cars by origin
   */

  def ex1() = {
    val carDS = readCars()

    carDS.filter(_.Horsepower.getOrElse(0L) > 140)
      .writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()

  }

  def ex2() = {
    val carDS = readCars()

    carDS.select(avg(col("Horsepower")))
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()

  }

  def ex3() = {
    val carDS = readCars()
    val carCountByOrigin = carDS.groupBy(col("Origin")).count() // option 1
    val carCountByOriginAlt = carDS.groupByKey(car => car.Origin).count() // option 2 with the Dataset API

    carCountByOriginAlt
      .writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args:Array[String]):Unit  ={
    ex3()
  }

}
