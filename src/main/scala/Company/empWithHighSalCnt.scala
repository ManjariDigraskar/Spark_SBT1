package Company

import org.apache.spark.sql
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import org.apache.spark.sql.functions.column
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

import scala.reflect.internal.util.TableDef.Column


object empWithHighSalCnt {

  def cntsal(col_Sal:DataFrame): Int = {
    val cnt=0
       //col_Sal.se
    cnt
  }

  def main(args:Array[String]): Unit = {
    val ss = SparkSession.builder()
      .master("local[2]")
      .appName("Cnt of Higher Salary than Employee")
      .getOrCreate()

    val df = ss.read
      .option("header", true)
      .option("inferSchema", true)
      .csv("data/emp_details.txt")

    df.show()

    val win1=Window.orderBy(asc("emp_sal"))
    val w1 = Window.rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
    System.out.println("Combined code")
    val df1 = df.select("emp_name","emp_sal")
      .withColumn("Rank",rank() over(win1))
      .withColumn("Rank1",max("Rank") over(w1))

      df1.withColumn("Cnt",df1("Rank1")-df1("Rank"))
        .select("emp_name","emp_sal","Cnt")
      .show()
  }
}
