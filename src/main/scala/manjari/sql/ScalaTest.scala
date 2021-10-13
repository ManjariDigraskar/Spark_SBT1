package manjari.sql

import manjari.scala.TapGlobal1.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object ScalaTest {
  @transient lazy val loggerTG : Logger= Logger.getLogger(getClass.getName)

  def main(args:Array[String])={
    loggerTG.info("Info")
    val spark= SparkSession.builder()
      .appName("Customer Wise Age")
      .master("local[2]")
      .getOrCreate()

    val rdd=spark.read
      .option("inferschema",true)
      .option("header",true)
      .csv("data/Product.txt")

    val sql_rdd=rdd.createOrReplaceTempView("Prod")
    val sql_df=spark.sql("select * from Prod")
    sql_df.show()

  }
}
