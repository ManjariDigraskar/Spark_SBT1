package manjari.Streaming

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions.{explode, split}

object SocketStreaming {
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


    //val wordsDF = df.select(explode(split(df("value")," ")).alias("word"))
   import spark.implicits._
    val wordsDF=df.as[String].flatMap(_.split(" "))
   val counts = wordsDF.groupBy("value").count()
    val query = counts.writeStream
      .format("console")
      .outputMode("complete")
      .start()


      query.awaitTermination()
  }

}
