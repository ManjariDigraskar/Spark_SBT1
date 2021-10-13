package Company.Synechron

import org.apache.spark.sql.SparkSession


object Program3 {
def main(args:Array[String]):Unit={

  val sc=SparkSession.builder()
    .master("local[2]")
    .appName("Program3")
    .getOrCreate()

   //sc.implicits._

  val df=sc.read
    .option("header",true)
    .option("InferSchema",true)
    .option("delimiter"," ")
    .csv("data/ThirdProgrm.txt")

     df.select("Person","Friend").show()
    System.out.println("Distinct records")
  df.distinct().show()

 val p_df= df.dropDuplicates("Person").select("Person").show()
  val f_df=df.dropDuplicates("Friend").select("Friend").show()


}
}
