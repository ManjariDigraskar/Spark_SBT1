package manjari.spark.SqlAgg

import org.apache.spark.sql.SparkSession

object Agg_By_Key_01 {
def main(args:Array[String]): Unit ={
  val sc=SparkSession.builder()
    .appName("Aggregate By Key 01")
    .master("local[2]")
    .getOrCreate()

  val rf=sc.read
    .option("inferSchema","true")
    .csv("data/stu_marks.txt")

  rf.printSchema()
  rf.collect.foreach(println)
  //rf.map(row=>row(0),rp)

  val stu_rdd=rf.groupBy("_c0")
    .max("_c2")
    .show()

  //val rdd=rf.toDF().map()

}
}
