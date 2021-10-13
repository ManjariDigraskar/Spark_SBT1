package manjari.sql1

import org.apache.log4j.Logger
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.functions._


case class txnds(txn_id:String,txn_dt:String,cust_id:String,txn_amt:Double,prodct_cat:String,product:String,city:String,state:String,payment_mode:String)

object usingDataSetAndFun {
  @transient lazy val log:Logger=Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    log.info("INFO")

    val spark=SparkSession.builder()
      .appName("DataSetAndFunc")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val txnDS1:Dataset[txnds] =spark.read
    .option("inferSchema",true)
      .option("header",true)
      .csv("data/txns1.txt").as[txnds]

  //txnDS1.printSchema()
    txnDS1.groupBy("cust_id")
      .sum("txn_amt")
      .sort(desc("sum(txn_amt)"))
      .limit(10)
      .show()


    println("States Starting with S")
    txnDS1.filter(col("state").startsWith("S")).show()

    println("Grouped with city and state")
    txnDS1.groupBy("city","state").sum("txn_amt").show()

    txnDS1.rdd.map(txn=>(txn.cust_id, txn.txn_amt > 100)).saveAsTextFile("C:/test")
  }
}
