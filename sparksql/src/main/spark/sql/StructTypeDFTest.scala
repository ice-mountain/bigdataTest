package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

object StructTypeDFTest {
  def main(args: Array[String]): Unit = {
    /**
      * 获取编程入口
      * sparksession 是2.x版本的 sqlContext 1.x版本
      */
    val sparkConf: SparkConf = new SparkConf()
    sparkConf.setMaster("local")
    sparkConf.setAppName("SparkSql_First")
    val sparkContext: SparkContext = new SparkContext(sparkConf)
    val sQLContext = new SQLContext(sparkContext)
    /**
      * 得到抽象数据
      */
    val wordArrayRDD: RDD[Array[String]] = sparkContext.textFile("hdfs://hadoop:9000/student/student").
      map(x => x.split(","))
    //真实数据
    val rowRDD: RDD[Row] = wordArrayRDD.map(x => Row(x(0).toInt, x(1), x(2), x(3).toInt, x(4)))
    //指定Schema信息
    val structType: StructType = StructType(Array(
      StructField("id", IntegerType, false),
      StructField("name", StringType, false),
      StructField("sex", StringType, false),
      StructField("age", IntegerType, false),
      StructField("department", StringType, false)
    ))
    val studentDF: DataFrame = sQLContext.createDataFrame(rowRDD, structType)
    //创建的view就等于table
    studentDF.createTempView("stu_view")
    val resultDF: DataFrame = sQLContext.sql("select distinct age from stu_view")
    // resultDF.show()
    resultDF.write.save(args(0))
    sparkContext.stop()
  }
}
