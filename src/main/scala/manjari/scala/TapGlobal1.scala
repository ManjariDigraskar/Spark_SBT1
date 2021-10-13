package manjari.scala

import manjari.sql.custage50.getClass
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession

object TapGlobal1 {
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

    rdd.printSchema()
    //Total Sales Per Month Per Product
    System.out.println("Total Sales Per Month Per Product")
      val rdd1=rdd.groupBy("Month","Product")
                 .sum("Sales").show()

    //Total avg sale Per Month Per Product
    System.out.println("Total avg Sales Per Month Per Product")
    val rdd2=rdd.groupBy("Month","Product")
      .avg("Sales").show()

    
    //Total Sales Per Month
    System.out.println("Total Sales Per Month")
    val rdd3=rdd.groupBy("Month")
      .sum("Sales").show()

    //Total Sales Product wise
        System.out.println("Total Sales Product Wise")
          val rdd4=rdd.groupBy("Product")
                     .sum("Sales").show()
  }
}
