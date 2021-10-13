package Company

import java.util.Date

import org.apache.arrow.vector.types.pojo.ArrowType.Timestamp
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType, TimestampType}

object Deutche_prg1 {
  @transient val log:Logger=Logger.getLogger(getClass.getName)
  def main(args:Array[String]):Unit={

    val client_schema=StructType(List(StructField("Id",IntegerType,nullable = false),
      StructField("account_number",IntegerType,nullable = false),
      StructField("DOJ",StringType,nullable = true),
      StructField("status",StringType,nullable = false)))

    val client_details_schema=StructType(List(StructField("account_number",IntegerType,nullable = false),
      StructField("name",StringType,nullable = false),
      StructField("address",StringType,nullable = false),
      StructField("created_date",StringType,nullable = false)))

    val ss=SparkSession.builder()
      .appName("Deutche Prg1")
      .master("local[2]")
      .getOrCreate()

    import ss.implicits._
    val client_df=ss.read
        .option("header",true)
      .schema(client_schema)
      .csv("data/client.txt")

    val client_df_dt=client_df.select(date_format(col("DOJ"), "yyyy-mm-dd"))

    val client_details_df=ss.read
      .option("header",true)
      .schema(client_details_schema)
      .csv("data/Client_Details.txt")

    val clientdetails_date_df=client_details_df.select(date_format(col("created_date"),"yyyy-mm-dd"))


    //Get the Active List
    val active_client=client_df.select("account_number","DOJ","status")
      .where(col("status")==="Active").toDF()//.show()

    //Get the latest details of the clients

    val window_client_details=Window.partitionBy("account_number")
      .orderBy($"created_date"desc)
    val filter_client_details=client_details_df
      .withColumn("Latest_dt",rank().over(window_client_details))

    val final_client_details=filter_client_details
      .select("account_number","name","address")
      .where(col("Latest_dt")=== 1)

    val res=final_client_details
        .join(active_client,"account_number")

      res.toDF("account_number","name","DOJ","address","status")

    //account_number, name, DOJ, address
    res.select("account_number","name","DOJ","address").show()

    System.out.println("Practice")
    val cl=ss.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/client.txt")

    val cl_dt=ss.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/Client_Details.txt")

    val fl_cl=cl.where(col("status")==="Active")

    val win=Window.partitionBy("account_number").orderBy("created_date")
    val fl_cl_dt=cl_dt.withColumn("dt_created_order",rank()over(win))
      .where(col("dt_created_order")===1)

    val res1=fl_cl.join(fl_cl_dt,"account_number")
      .select("account_number","name","DOJ","address")
      .show()
  }
}
