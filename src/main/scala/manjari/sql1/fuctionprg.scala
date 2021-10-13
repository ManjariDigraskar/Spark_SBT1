package manjari.sql1

import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object fuctionprg {
  @transient lazy val log:Logger=Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
    log.info("Info")

    val cs=createSpark()
    val cdfo=createDF(cs,"data/custs.txt")
    showcusts(cdfo)
  }
  def createSpark(): SparkSession ={
    val sparksess=SparkSession.builder()
      .appName("Using Functions")
      .master("local[2]")
      .getOrCreate()
    sparksess
  }

  def createDF(ss : SparkSession,filename:String):DataFrame={
  val df=ss.read
    .option("ifernSchema",true)
    .option("header",true)
    .csv(filename)
df
  }
  def showcusts(cdf:DataFrame):Unit={
    println("Before Partition"+cdf.rdd.getNumPartitions)
    val newcdf=cdf.repartition(2)
    println("Before Partition"+newcdf.rdd.getNumPartitions)
    cdf.show(20)
  }
}










