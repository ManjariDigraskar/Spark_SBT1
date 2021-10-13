package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object fname_startWithS {
  @transient lazy val logger:Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger.info("fname_startWithS")
    logger.warn("fname_startWithS")

    val spark=SparkSession.builder()
      .appName("fname_startWithS")
      .master("local[2]")
      .getOrCreate()

    val fnamedf= spark.read
      .option("infernSchema",true)
      .option("header",true)
      .csv("data/custs.txt")

   // fnamedf.select("f_name","l_name").where("f_name LIKE 'S%'").show()
    //fnamedf.filter(fnamedf("f_name") LIKE 'S%')
    fnamedf.where(col("f_name") startsWith("S")).show()
  }

}
