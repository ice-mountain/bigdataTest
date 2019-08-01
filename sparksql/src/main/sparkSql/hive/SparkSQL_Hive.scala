package sparkSql.hive

import org.apache.spark
import org.apache.spark.sql.{DataFrame, SparkSession}

object SparkSQL_Hive {
  def main(args: Array[String]): Unit = {
    val sparkSession: SparkSession = new spark.sql.SparkSession.Builder().
      appName("SparkSQL_Hive").master("local")
      .enableHiveSupport().getOrCreate()
    val stuDF: DataFrame = sparkSession.sql("select * from big.student")
    stuDF.show()
    sparkSession.stop()
  }
}
