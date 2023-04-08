package part1Basics

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object SparkBasics {

  // Entry point to the Spark Structured API
  val spark = SparkSession.builder()
    .appName("Spark Basics")
    .master("local[2]")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  // read a DF
  val cars = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/cars")



  import spark.implicits._

  // select
  val usefulCarsData = cars.select(
    col("Name"), // column object
    $"Year", // another column object (need spark implicits)
    (col("Weight_in_lbs") / 2.2).as("Weight_in_kgs"),
    expr("Weight_in_lbs/ 2.2").as("Weight_in_kg_2")
  )

  // select Expr
  val carsWeights = cars.selectExpr("Weight_in_lbs / 2.2")

  // filter
  val europeanCars = cars.where(col("Origin") =!= "USA")

  // aggregations
  val averageHP =  cars.select(avg(col("Horsepower")).as("average_hp"))

  // grouping
  val countByOrigin = cars.groupBy("Origin").count()

  // joining
  val guitarPlayers = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/guitarPlayers")

  val bands = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  val guitaristsBands = guitarPlayers.join(bands,guitarPlayers.col("band") === bands.col("id"))
  /*
      join types
      - inner: only the matching rows are kept
      - left/right/full outer join
      - semi/anti
     */

  // datasets = typed distributed collection of objects
  case class  GuitarPlayer(id:String,name:String,guitars:Seq[Long],band:Long)
  val guitarPlayersDS = guitarPlayers.as[GuitarPlayer] // need spark implicits
  guitarPlayersDS.map(_.name)

  // Spark SQL
  cars.createOrReplaceTempView("cars")
  val americanCars = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      """.stripMargin
  )

  // low level API: RDDs

  val sc = spark.sparkContext
  val numbersRDD: RDD[Int] = sc.parallelize( 1 to 1000000)

  // functional operators
  val doubles = numbersRDD.map(_ * 2)

  // RDD -> DF
  val numbersDF = numbersRDD.toDF("number") // you lose type info, you get SQL capability

  // RDD -> DS
  val numberDS =  spark.createDataset(numbersRDD)

  // DS -> RDD
  val guitarPlayersRDD = guitarPlayersDS.rdd

  // DF -> RDD
  val carsRDD = cars.rdd // RDD[Row]


  def main(args:Array[String]):Unit = {
    // showing a DF to the console
    cars.show(5)
    usefulCarsData.show(5)
    europeanCars.show(5)
    averageHP.show()
    countByOrigin.show(5)
    guitaristsBands.show(5)

  }
}
