package manjari.Streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.concurrent.duration._
object DataStreaming {

  @transient lazy val log:Logger=Logger.getLogger(getClass.getName)

  val spark = SparkSession
    .builder()
    .appName("Our first streams")
    .master("local[2]")
    .getOrCreate()

  def readFromSocket() = {
    //reading dataFrame
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()
    //Add all transforation u want
    //here, in streaming DF, you can do almost all the things which you can do with the Static DataFrame
    val shortLines:DataFrame = lines.filter(length(col("value"))<=5)

    //is this the regular static DF or Streaming DF...check below property
    if(shortLines.isStreaming )
      println("This is streaming API")
    else
      println("This is not Streaming API")

    //Consuming a DF
    //    val query = lines.writeStream
    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    //Wait for the stream to finish
     query.awaitTermination()
  }


  def readFromFiles() = {

    val stocksDF: DataFrame = spark.readStream
      .format("csv")
      .option("header", "false")
      .option("dateFormat", "MMM d yyyy")
      .option("inferSchema", true)
      .load("data/stocks")

    stocksDF.writeStream
      .format("console")
      .outputMode("append")
      .start()
      .awaitTermination()
  }

  def demoTriggers() = {
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 12345)
      .load()

    // write the lines DF at a certain trigger
    lines.writeStream
      .format("console")
      .outputMode("append")
      .trigger(
        Trigger.ProcessingTime(2.seconds) // every 2 seconds run the query
        // Trigger.Once() // single batch, then terminate
        //Trigger.Continuous(2.seconds) // experimental, every 2 seconds create a batch with whatever you have
      )
      .start()
      .awaitTermination()
  }

  def main(args: Array[String]): Unit = {
    log.info("INFO")
    log.warn("WARN")
    log.error("ERROR")
      //  readFromFiles()
    //  demoTriggers()
    readFromSocket()
  }


}
