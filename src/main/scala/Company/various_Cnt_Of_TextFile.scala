package Company

import org.apache.spark.{SparkConf, SparkContext}

object various_Cnt_Of_TextFile {
  def main(args:Array[String]):Unit={
    val conf=new SparkConf()
      .setAppName("Various count of text file")
      .setMaster("local[2]")

    val sc=new SparkContext(conf)

    val rdd=sc.textFile("data/story_text.txt")

    //count no of lines
    val no_of_lines=rdd.count()
    System.out.println("No Of Lines="+no_of_lines)

    //count no of words
     val no_of_words=rdd.flatMap(_.split(" ")).count()
    System.out.println("No Of Words="+no_of_words)

    //count of no of paragraphs
    val no_of_para=rdd.flatMap(_.split(".")).count()
    System.out.println("No Of Paragraphs="+no_of_para)

    //count of no of characters
    val initval=0
   val char_rdd=rdd.flatMap(_.split(""))
      .map(arr=>(arr,1)).reduceByKey(_+_).collect()//.foreach(println)
    val no_of_char=char_rdd.map(arr=>arr._2).aggregate(initval)(seqOp, combOp)
    System.out.println("No Of Characters="+no_of_char)

  }
  def seqOp = (acc: Int, ele: Int) => acc + ele

  //Combiner Operation
  def combOp = (accumulator1: Int, accumulator2: Int) =>
    accumulator1 + accumulator2
}
