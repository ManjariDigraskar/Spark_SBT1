package manjari.spark.sql

import org.apache.avro.generic.GenericData.StringType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}

object nullDataExample {
def main(args:Array[String])={

  val ss=SparkSession.builder()
    .master("local[2]")
    .appName("Null Data Example")
    .getOrCreate()
  //,age,country,zip_code,date
  //val schema=StructType(List(StructField("name",StringType,true)))
  }
}
