package spark.sql

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row, SQLContext, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkSQL_JDBC {
  def main(args: Array[String]): Unit = {
    /**
      * 获取编程入口
      * sparksession 是2.x版本的 sqlContext 1.x版本
      */
   val sparkSession = SparkSession.builder().master("local").appName("SparkSQL_JDBC").getOrCreate()
    /**
      * 得到抽象数据
      */
    val studentDateFrame: DataFrame = sparkSession.read.format("csv")
      .options(Map("sep"->",","inferSchema"->"true","header"->"true"))
      .load("hdfs://hadoop:9000/student/student.txt")

    studentDateFrame.write.format("jdbc")
      .option("url", "jdbc:mysql://hadoop:3306/test")
      .option("dbtable", "stu1").option("user", "root")
      .option("password", "123456").mode("append").save()
    sparkSession.stop()
  }
}
