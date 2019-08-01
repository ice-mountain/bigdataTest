package sparkSql.hive

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

object MyAvg extends UserDefinedAggregateFunction{
  /**
    * 当前这个函数的输出参数类型
    * @return
    */
  override def inputSchema: StructType = StructType({
    StructField("age",DoubleType,false)::Nil
  })

  /**
    * 辅助变量
    * 年龄总和/个数
    * @return
    */
  override def bufferSchema: StructType =StructType({
    StructField("total",DoubleType,false)::StructField("count",IntegerType,false)::Nil
  })

  /**
    * 初始化辅助变量这里面装的是辅助变量
    * @param buffer
    */
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer.update(0,0.0)
    buffer.update(1,0)
  }

  /**
    * 这既是迭代操作
    * @param buffer buffer  count
    * @param input myavg(age) input里面包含的就是age这个字段的值
    */
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    //取出原来值
    val total: Double = buffer.getDouble(0)
    val count: Int = buffer.getInt(1)
    //迭代操作
    val newtotal: Double =total+input.getDouble(0)
    val newcount: Int =count+1
    //重新赋值
    buffer.update(0,newtotal)
    buffer.update(1,newcount)
  }

  /**
    *合并
    * @param buffer1
    * @param buffer2
    */
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1.update(0,buffer1.getDouble(0)+buffer2.getDouble(0))
    buffer1.update(1,buffer1.getInt(1)+buffer2.getInt(1))
  }

  /**
    *输出的值得类型
    * @return
    */
  override def dataType: DataType ={DoubleType}

  /**
    * 输出值得类型和输出值得类型是否一样
    * @return
    */
  override def deterministic: Boolean = true

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)/buffer.getInt(1)
  }
}
