package Company.Synechron

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.countDistinct


object Program1 {
 def main(args:Array[String]):Unit={

  val sc=SparkSession.builder()
    .master("local[2]")
    .appName("Program1")
    .getOrCreate()

  val df=sc.read
    .option("header",true)
    .option("InferSchema",true)
    .option("delimiter","|")
    .csv("data/SparkCountPrg.txt")

  System.out.println(df.printSchema())

  val filterdf=df.select("Col1").groupBy("Col1").count().show()
 }
}
