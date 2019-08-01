package spark.sql

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ：Administrator
  **/
object SparkSql_First {
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
    val studnetRDD: RDD[Student] = wordArrayRDD.map(x => Student(x(0).toInt, x(1), x(2), x(3).toInt, x(4)))
    // val dataFrame: DataFrame = sQLContext.createDataFrame(studnetRDD)
    import sQLContext.implicits._
    val dataFrame: DataFrame = studnetRDD.toDF() //也可以得到DataFrame

    /**
      * 对dataFrame数据进行操作DSL风格
      */
    //dataFrame.show()
    //dataFrame.select("id","name").show()
    /**
      * sql风格
      */
    dataFrame.registerTempTable("student")
    val sqlDateFrame: DataFrame =
      sQLContext.sql("select department,count(*) as total from student group by department")
    sqlDateFrame.show()
    sparkContext.stop()
  }
}

case class Student(id: Int, name: String, sex: String, age: Int, department: String)
