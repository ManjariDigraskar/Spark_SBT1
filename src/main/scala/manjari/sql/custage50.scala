package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

object custage50 {
  @transient lazy val logger1 : Logger= Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger1.info("test2")

//System.setProperty("hadoop.home.dir","C:/hadoop/bin")
    val spark= SparkSession.builder()
      .appName("Customer Wise Age")
      .master("local[2]")
      .getOrCreate()

    val cage=spark.read
      .option("inferschema",true)
      .option("header",true)
      .csv("data/custs.txt")

    System.out.println("Using SQL")
    val sql_q=cage.createOrReplaceTempView("cust")
    val sql_q1=spark.sql("Select f_name,l_name,age from cust where age>50")
    sql_q1.show()

    val query="age > 50"
    cage.select("f_name","l_name","age").where(query).show()
    for(i<-cage)
    cage.filter(cage("age")>=50).show()

  }
}
