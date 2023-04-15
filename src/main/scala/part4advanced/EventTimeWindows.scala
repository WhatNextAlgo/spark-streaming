package part4advanced
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object EventTimeWindows {
  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("Event Time Window")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val onlinePurchaseSchema = StructType(Array(
    StructField("id",StringType),
    StructField("time",TimestampType),
    StructField("item",StringType),
    StructField("quantity",IntegerType)
  ))

  def readPurchasesFromSocket() = {
    spark.readStream
      .format("socket")
      .option("host", "127.0.0.1")
      .option("port", 1234)
      .load()
      .select(from_json(col("value"), onlinePurchaseSchema).as("purchase"))
      .selectExpr("purchase.*")
  }

  def readPurchasesFromFile() = spark.readStream
    .schema(onlinePurchaseSchema)
    .json("src/main/resources/data/purchases")

  def aggregatePurchasesByTumblingWindow() = {
    val purchaseDF = readPurchasesFromSocket()

    val windowByDay = purchaseDF
      .groupBy(window(col("time"),"1 day").as("time")) //tumbling = sliding window == window duration
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def aggregatePurchasesBySlidingWindow() = {
    val purchaseDF = readPurchasesFromSocket()

    val windowByDay = purchaseDF
      .groupBy(window(col("time"), "1 day","1 hour").as("time")) // struct column: has fields {start, end}
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("totalQuantity")
      )

    windowByDay.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  /**
   * Exercises
   * 1) Show the best selling product of every day, +quantity sold.
   * 2) Show the best selling product of every 24 hours, updated every hour.
   */

  def bestSellingProductByDay() = {
    val purchaseDF = readPurchasesFromFile()

    val bestSelling = purchaseDF
      .groupBy(window(col("time"),"1 day").as("day"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("day").getField("start").as("start"),
        col("day").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      ).orderBy(col("start"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def bestSellingProductEvery24h() = {
    val purchasesDF = readPurchasesFromFile()

    val bestSelling = purchasesDF
      .groupBy(col("item"), window(col("time"), "1 day", "1 hour").as("time"))
      .agg(sum("quantity").as("totalQuantity"))
      .select(
        col("time").getField("start").as("start"),
        col("time").getField("end").as("end"),
        col("item"),
        col("totalQuantity")
      )
      .orderBy(col("start"), col("totalQuantity").desc)

    bestSelling.writeStream
      .format("console")
      .outputMode("complete")
      .start()
      .awaitTermination()
  }

  def main(args:Array[String]):Unit = {
    bestSellingProductEvery24h()
  }
}


