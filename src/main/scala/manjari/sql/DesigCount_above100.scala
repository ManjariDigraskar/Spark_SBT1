package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object DesigCount_above100 {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("INFO")
    logger.error("Error")

    val spark= SparkSession.builder()
      .appName("DesigCount_above100")
      .master("local[2]")
      .getOrCreate()

    val desDF= spark.read
      .option("infernSchema",true)
      .option("header",true)
      .csv("data/custs.txt")

    desDF.groupBy("desig").count().alias("count").where("count >100").show()
  }

}
