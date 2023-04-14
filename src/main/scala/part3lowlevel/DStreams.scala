package part3lowlevel

import java.io.{File,FileWriter}
import java.sql.Date
import java.text.SimpleDateFormat

import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.{Seconds,StreamingContext}
import common._
object DStreams {

  val spark = SparkSession.builder()
    .config("spark.master","local")
    .appName("DStreams")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  /*
      Spark Streaming Context = entry point to the DStreams API
      - needs the spark context
      - a duration = batch interval
     */

  val ssc = new StreamingContext(sparkContext = spark.sparkContext, batchDuration = Seconds(1))
  /*
      - define input sources by creating DStreams
      - define transformations on DStreams
      - call an action on DStreams
      - start ALL computations with ssc.start()
        - no more computations can be added
      - await termination, or stop the computation
        - you cannot restart the ssc
     */

  def readFromSocket() = {
    Thread.sleep(3000)
    val socketStream:DStream[String] = ssc.socketTextStream("127.0.0.1",1234)

    // transformation = lazy
    val wordsStream:DStream[String] = socketStream.flatMap(line => line.split(" "))

    //action
    wordsStream.print()
//    wordsStream.saveAsTextFiles("src/main/resources/data/words/") // each folder = RDD = batch, each file = a partition of the RDD

    ssc.start()
    ssc.awaitTermination()
  }

  def createNewFile() = {
    new Thread(() =>{
      Thread.sleep(5000)
      val path = "src/main/resources/data/stocks"
      val dir = new File(path)
      val nFiles = dir.listFiles().length
      val newFile = new File(s"$path/newStock$nFiles.csv")
      newFile.createNewFile()

      val writer = new FileWriter(newFile)
      writer.write(
        """
          |AAPL,Sep 1 2000,12.88
          |AAPL,Oct 1 2000,9.78
          |AAPL,Nov 1 2000,8.25
          |AAPL,Dec 1 2000,7.44
          |AAPL,Jan 1 2001,10.81
          |AAPL,Feb 1 2001,9.12
                """.stripMargin.trim)
      writer.close()
    }).start()
  }

  def readFromFile() ={
//    createNewFile()

    // defined DStream
    val stocksFilePath = "src/main/resources/data/StockInput"
    /*
    ssc.textFileStream monitors a directory for NEW FILES

     */

    val textStream:DStream[String] = ssc.textFileStream(stocksFilePath)

    // transformation
    val dateFormat = new SimpleDateFormat("MMM d yyyy")

    val stocksStream: DStream[Stock] = textStream.map { line =>
      val tokens = line.split(",")
      val company = tokens(0)
      val date = new Date(dateFormat.parse(tokens(1)).getTime)
      val price = tokens(2).toDouble

      Stock(company, date, price)

    }
    // action
    stocksStream.print()

    // start the computations
    ssc.start()
    ssc.awaitTermination()
  }
  def main(args:Array[String]):Unit = {
    readFromFile()
  }
}
