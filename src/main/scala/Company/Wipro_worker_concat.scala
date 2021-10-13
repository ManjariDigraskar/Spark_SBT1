package Company

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

case class wrkr(WORKER_ID:Integer,FIRST_NAME:String,LAST_NAME:String,SALARY:String,JOINING_DATE:String,DEPARTMENT:String)

object Wipro_worker_concat {
  def main(args:Array[String]): Unit ={
    val ss=SparkSession.builder()
      .master("local[2]")
      .appName("Wipro Concatination of String")
      .getOrCreate()

    import ss.implicits._

    val wrkr_schema=StructType.fromDDL("WORKER_ID int ,FIRST_NAME string,LAST_NAME string,SALARY float,JOINING_DATE date,DEPARTMENT string")

    val wrkr_ds=ss.read
        .format("csv")
      .option("header",false)
      .option("inferSchema",false)
      .schema(wrkr_schema)
      .load("data/worker.txt")
        .as[wrkr]

    wrkr_ds.show()
    wrkr_ds.select("WORKER_ID","FIRST_NAME","LAST_NAME","DEPARTMENT")
  }
}
