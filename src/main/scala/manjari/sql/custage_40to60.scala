package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object custage_40to60 {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("INFO")
    logger.error("Error")

    val spark=SparkSession.builder()
      .appName("custage_40to60")
      .master("local[2]")
      .getOrCreate()

    val custageDF=spark.read
      .option("infernSchema",true)
      .option("header",true)
      .csv("data/custs.txt")

    custageDF.where("age > 40 AND age < 60").show()
    custageDF.filter(custageDF("age")>=40 and custageDF("age")<=60).show()
  }

}
