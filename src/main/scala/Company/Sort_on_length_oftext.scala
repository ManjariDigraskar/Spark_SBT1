package Company

import org.apache.spark.{SparkConf, SparkContext}

object Sort_on_length_oftext {
       def main(args:Array[String]):Unit={
         val conf=new SparkConf()
           .setMaster("local[2]")
           .setAppName("Sort on Length of String")

         val sc=new SparkContext(conf)
         var arr=new Array[String] (5)
         arr=Array("dog","tiger","lion","cat","panther","eagle")
         val rdd=sc.parallelize(arr)
         val initval=0
         val initstr=""

         rdd.flatMap(_.split(","))
           .collect.foreach(println)

        val res_rdd= rdd.map(_.split(",")).map(arr => (arr(0).size, arr(0)))
          res_rdd.sortBy(arr=>arr._1).groupByKey().sortBy(arr=>(arr._1)).collect.foreach(println)
  }
}
