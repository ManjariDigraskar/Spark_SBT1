package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object DesignationWiseCount {
  @transient lazy val logger: Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {

  logger.info("test")
  logger.error("Error")

    val spark=SparkSession.builder()
     .appName("DesigWiseCount")
      .master("local[2]")
      .getOrCreate()

    val custDF=spark.read
      .option("inferSchema",true)
      .option("header",true)
      .csv("data/custs.txt")

    /*|-- cid: integer (nullable = true)
 |-- f_name: string (nullable = true)
 |-- l_name: string (nullable = true)
 |-- age: integer (nullable = true)
 |-- desig: string (nullable = true)*/
   custDF.groupBy("desig").count().show()
  }

}
