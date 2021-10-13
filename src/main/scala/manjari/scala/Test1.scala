package manjari.scala

import org.apache.spark.{SparkConf, SparkContext}


object Test1 {
  def main(args:Array[String]): Unit ={
    val conf=new SparkConf()
      .setMaster("local[2]")
      .setAppName("Testing Word Count")

    val ss=new SparkContext(conf)

    //val rdd=ss.textFile("data/word.txt")
    val rdd=ss.textFile(args(0))
    rdd.flatMap(row=>row.split(" "))
      .map(wrd=>(wrd,1))
      .reduceByKey(_+_)
      .foreach(println)
  }
}
