package Company

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank

object Salary2ndHighest {
  @transient lazy val logger1 : Logger= Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    logger1.info("2nd Highest Salary")

    //System.setProperty("hadoop.home.dir","C:/hadoop/bin")
    val spark = SparkSession.builder()
      .appName("Customer Wise Age")
      .master("local[2]")
      .getOrCreate()

    import spark.implicits._

    val sal_rdd = spark.read
      .option("inferschema", true)
      .option("header", true)
      .csv("data/emp_details.txt")

    val high_2nd_sal = sal_rdd.toDF("emp_id","emp_name","emp_sal","dep_id")//.printSchema()

    val par_window=Window.partitionBy($"dep_id").orderBy($"emp_sal".desc)
    val rank_window_part=rank().over(par_window)

    val res_high_sal=high_2nd_sal.select($"*",rank_window_part as "highest_sal_rank")
      .where($"highest_sal_rank"===2)//.show()
    /*System.out.println("Second Highest Salary")
    res_high_sal.show()*/

    val emp_details_rdd=spark.read
      .option("inferSchema",true)
      .option("header",true)
      .csv("data/emp_dep.txt")

    val dt_rdd=emp_details_rdd.toDF()

    //val res_rdd=dt_rdd.join(high_2nd_sal,high_2nd_sal("dep_id")===dt_rdd("dep_id"))//,"dep_id")
    val res_rdd=res_high_sal.join(dt_rdd,"dep_id")
    System.out.println("Join Tables")
    res_rdd.show()
  }
}
