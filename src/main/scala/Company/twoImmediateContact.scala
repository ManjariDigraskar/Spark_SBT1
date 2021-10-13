package Company

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col

object twoImmediateContact {
  def main(args:Array[String]): Unit ={
    val ss=SparkSession.builder()
      .master("local[2]")
      .appName("Two Immediate Contact")
      .getOrCreate()

    val df=ss.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/Linkedin.txt")

    val boss_df=ss.read
      .option("header",true)
      .option("inferSchema",true)
      .csv("data/BossId.txt")
    import ss.implicits._

    //Joining the data of Linkedin and Bossid to get the empid,fn,bossid
    val linked_df=df.select("empid","fn")
    val join_df=linked_df.join(boss_df,"empid")
      join_df.show()

    //Joining the dataframe to get the bossname of the empl
    val boss_name=join_df.select("empid","fn","Bossid")

    val bn=boss_df.join(df,boss_df("Bossid")===df("empid"),"leftouter")
            .withColumn("First Connect",col("fn"))
        .select("Bossid","First Connect")
       bn.show()

  System.out.println("First Connect list")
    val final_df=bn.as("b1").join(boss_name.as("j1"),$"b1.Bossid"===$"j1.Bossid")//,"leftanti")
      .select($"j1.empid",$"j1.fn",$"b1.Bossid",$"b1.First Connect")
        .dropDuplicates()
      final_df.show()

    //Second connect
    System.out.println("Second Connect list")
    val sec_conn=final_df.select("Bossid","First Connect")
      val df1= sec_conn.join(df,sec_conn("First Connect")===df("fn"),"leftouter")
        .select("empid","First Connect")

    df1.show()

    val sec_boss=boss_df.join(df1,boss_df("empid")===df1("empid"),"rightouter")
    sec_boss.show()
    sec_boss.select("First Connect","Bossid")
      .show()

     val sec_conn1=df.join(sec_boss,sec_boss("Bossid")===df("empid"),"inner")
      .withColumn("Second Connect",col("fn"))
      .select("First Connect","Bossid","Second Connect")
    sec_conn1.show()

    System.out.println("List with both connects")
    val conn_link=final_df.as("d1").join(sec_conn1.as("d2"),$"d1.First Connect"===$"d2.First Connect","full")
        .select($"d1.empid",$"d1.fn",$"d1.First Connect",$"d2.Second Connect")
        .dropDuplicates()
    conn_link.show()
  }
}