package Company

import java.util.logging.Logger

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.rank

object HighestSalary {
  @transient lazy val log:Logger=Logger.getLogger(getClass.getName)
  def main(args:Array[String]):Unit={
    log.info("Information")
    //log.warning("Warning")

    val ss=SparkSession.builder()
      .appName("Highest Salary")
      .master("local[2]")
      .getOrCreate()

    import ss.implicits._

    val emp_sal=ss.read
      .option("header",true)
      .option("Inferschema",true)
      .csv("data/emp_details.txt")

    val highest_sal=emp_sal.toDF("emp_id","emp_name","emp_sal","dep_id")

    val window_part=Window.partitionBy($"dep_id").orderBy($"emp_sal".desc)

    val sal_rank=rank().over(window_part)

    //val sal=highest_sal.select($"*",sal_rank as "Sal Order")
    val sal=highest_sal.withColumn("Sal Order",sal_rank).where($"Sal Order"===1)
      //.where($"Sal Order"===1)

    sal.show()
  }
}
