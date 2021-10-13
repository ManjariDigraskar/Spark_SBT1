package manjari.SqlAgg

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, to_date}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}

object shrikantcode extends Serializable{

  @transient lazy val log: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    log.info("INFO")
    log.warn("WARN")
    log.error("ERROR")

    val spark = SparkSession.builder()
      .appName("Txn Schema")
      .master("local[2]")
      .getOrCreate()

    val txnSchema = StructType(List(
      StructField("txn_id", IntegerType, nullable = false),
      StructField("txn_dt", StringType, nullable = false),
      StructField("cust_id", StringType, nullable = false),
      StructField("txn_amt", DoubleType, nullable = false),
      StructField("product_cat", StringType, nullable = false),
      StructField("product", StringType, nullable = false),
      StructField("city", StringType, nullable = false),
      StructField("state", StringType, nullable = false),
      StructField("payment_mode", StringType, nullable = false)
    ))

    val txn = spark.read
      .format("csv")
      .option("header", true)
      .option("path", "data/manjiri.txt")
      .option("badRecordsPath", "data/badRecords")
      .schema(txnSchema)
      .load()

    print(txn.first())

    txn.printSchema()
    // .option("dateFormat","M-D-Y")

    //06-26-2011
    val newTxn=toDateDf(txn,"MM-dd-yyyy","Txn_Dt")
    println("With Date Schema")
    newTxn.printSchema()

    print(newTxn.first)

  }
  def toDateDf(txn: DataFrame, txndt: String, Txn_Dt: String): DataFrame = {
    txn.withColumn(Txn_Dt, to_date(col("Txn_Dt"), txndt))
  }
}
