package Company

import org.apache.spark.sql.SparkSession

object MissingCountryCode {
  def main(args:Array[String]): Unit ={
   val ss=SparkSession.builder()
     .master("local[2]")
     .appName("MissingCountryCode")
     .getOrCreate()

    val ctry_df=ss.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/country.csv")

    val emp_df=ss.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/emp_ctry.csv")

    val emp_df_ctr=emp_df.select("country_code") as ("emp ctry_code")

    val joindf=emp_df_ctr.join(ctry_df,emp_df("country_code")===ctry_df("country_code"),"leftanti")
      .show()

    //val empctr=emp_df.select("country_code") as ("emp ctry")

    val joinDf=emp_df.join(ctry_df,emp_df("country_code")===ctry_df("country_code"),"leftanti")
      .select("country_code")
      .show()
  }
}
