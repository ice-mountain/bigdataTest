package sparkSql.hive

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.HiveContext


object SparkSQL_Hive2 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("SparkSQL_Hive2")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val hiveContext: HiveContext = new HiveContext(sparkContext)
    val stuDF: DataFrame = hiveContext.sql("select * from big.student")
    stuDF.show()
    sparkContext.stop()
  }
}
