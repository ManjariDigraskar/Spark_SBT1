package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object Missing_Desig {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("Missing_Desig")
    logger.warn("Missing_Desig")

    val spark=SparkSession.builder()
      .appName("Missing_Desig")
      .master("local[2]")
      .getOrCreate()

    val miss_desig=spark.read
      .option("inferSchema",true)
      .option("header",true)
      .csv("data/custs.txt")

    val cnt=miss_desig.where("desig IS NULL").count()
    println("Missing Designation Count="+cnt)
  }

}
