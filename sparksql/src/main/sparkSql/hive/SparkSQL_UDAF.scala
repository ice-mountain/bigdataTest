package sparkSql.hive

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_UDAF{
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = new spark.sql.SparkSession.Builder()
      .master("local").appName("SparkSQL_UDF").enableHiveSupport()
      .getOrCreate()
    sparkSession.udf.register("myavg",MyAvg)
    val stuDf: DataFrame = sparkSession.sql("select myavg(age) as newage from big.student")
    stuDf.show()
    sparkSession.stop()
  }
}
