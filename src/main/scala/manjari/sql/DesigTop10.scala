package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.desc

object DesigTop10 {
  @transient lazy val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("DesigTop10")
    logger.warn("DesigTop10")

    val spark = SparkSession.builder()
      .appName("DesigTop10")
      .master("local[2]")
      .getOrCreate()

    val topdesig = spark.read
      .option("inferSchema", true)
      .option("header", true)
      .csv("data/custs.txt")
    topdesig.groupBy("desig").count().as("count")
            .sort(desc("count")).show(10)

    val top2ndDesig=spark.read
      .option("inferSchema",true)
      .option("header",true)
      .csv("data/custs.txt")

    top2ndDesig.groupBy("desig").count().as("cnt")
      .sort(desc("cnt")).show(5)

  }
}
