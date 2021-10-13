package manjari.Streaming

import SocketStreaming.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.functions.{col, length}
import org.apache.spark.sql.{DataFrame, SparkSession}

object Stream1 {
  @transient lazy val log:Logger=Logger.getLogger(getClass.getName)
  def main(args:Array[String]):Unit={
    val spark:SparkSession = SparkSession.builder()
      .master("local[2]")
      .appName("SparkByExample")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val df :DataFrame= spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","12345")
      .load()

    println("spark streaming started")


    val shortLines:DataFrame = df .filter(length(col("value"))<=5)

    if(shortLines.isStreaming )
      println("This is streaming API")
    else
      println("This is not Streaming API")

    val query = shortLines.writeStream
      .format("console")
      .outputMode("append")
      .start()

    //Wait for the stream to finish
    query.awaitTermination()
  }


}
