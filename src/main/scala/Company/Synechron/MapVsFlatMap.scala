package Company.Synechron

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object MapVsFlatMap {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
      .setAppName("MapVsFlatMap")
      .setMaster("local[2]")

    val sc=new SparkContext(conf)

    val rdd=sc.textFile("data/SparkCountPrg.txt")
    System.out.println("RDD="+rdd.count())

    val flatMaprdd=rdd.flatMap(_.split("|"))
       System.out.println("FlatMap = "+flatMaprdd.count())

    val maprdd=rdd.map(_.split("|"))
    System.out.println("Map = "+maprdd.count())
  }
}
