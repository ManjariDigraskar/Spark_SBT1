package Company.Synechron

import org.apache.spark.sql.SparkSession

object sal_dep {
def main(args:Array[String]):Unit={
  val ss=SparkSession.builder()
    .appName("Sal Dep")
    .master("local[2]")
    .getOrCreate()

  val e_df=ss.read
    .option("header",true)
    .option("InferSchema",true)
    .option("delimiter",",")
    .csv("data/emp_details.txt")

  val d_df=ss.read
    .option("header",true)
    .option("InferSchema",true)
    .option("delimiter",",")
    .csv("data/emp_dep.txt")

  val r_df=e_df.select("emp_id","emp_name","emp_sal","emp_dep_id")
    .orderBy("emp_sal")
    .limit(1)
    .join(d_df,e_df("emp_dep_id")===d_df("dep_id"))

  r_df.select("emp_id","emp_name","emp_sal","dep_name").show()
}
}
