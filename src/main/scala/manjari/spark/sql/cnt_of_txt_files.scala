package manjari.spark.sql


import org.apache.spark.{SparkConf, SparkContext}

object cnt_of_txt_files {

  def main(args:Array[String]): Unit ={

    val conf=new SparkConf()
      .setAppName("Text Counts")
      .setMaster("local[2]")

    val sc=new SparkContext(conf)

    val txt=sc.textFile("data/story_text.txt")
   //val txt=sc.textFile("data/word.txt")
    txt.flatMap(f=>f.split(""))
      .map(wrd=>(wrd,1))
      .reduceByKey(_+_)
      .foreach(println)
    sc.stop()
    
  }
}
