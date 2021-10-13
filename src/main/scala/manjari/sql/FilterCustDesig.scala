package manjari.sql

import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object FilterCustDesig {
    @transient lazy val logger: Logger= Logger.getLogger(getClass.getName)

    def main(args: Array[String]): Unit = {

        logger.info("Filter Desig")
        logger.warn("Filter Desig warn")

        val spark= SparkSession.builder()
          .appName("Filter Customer Desingnation")
          .master("local[2]")
          .getOrCreate()

        val cust_desig=spark.read
          .option("inferschema",true)
          .option("header",true)
          .csv("data/custs.txt")

       // cust_desig.select("f_name","l_name","desig").where("desig='Teacher' OR desig='Pilot'").show()
        cust_desig.where(col("desig")==="Teacher" or col("desig")=== "Pilot").show(10)
    }

}
