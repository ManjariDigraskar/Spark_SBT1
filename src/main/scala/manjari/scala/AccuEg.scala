package manjari.scala

import org.apache.spark.sql.SparkSession

object AccuEg {
  def main(args:Array[String]): Unit =
  {
    val sc=SparkSession.builder()
      .appName("Accumulator eg")
      .master("local")
      .getOrCreate()

    val bc=sc.sparkContext.broadcast("Welcome Message")

    val acc=sc.sparkContext.longAccumulator("Accumulators")
    val rdd=sc.sparkContext.parallelize(Array(3, 4, 5))
    rdd.foreach(rec => acc.add(rec))

    println(acc.value)
    println(acc.name)

    println(bc.value)

  }

}
