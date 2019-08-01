package sparkSql.hive

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_UDF {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = new spark.sql.SparkSession.Builder()
      .master("local").appName("SparkSQL_UDF").enableHiveSupport()
      .getOrCreate()
    sparkSession.udf.register("age2xxx", (x:Int)=>x*2)
    val stuDf: DataFrame = sparkSession.sql("select age2xxx(age) as newage,name from big.student")
    stuDf.show()
    sparkSession.stop()
  }
}
