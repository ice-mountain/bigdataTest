package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

/**
  * 默认情况下程序停止了是不会保存StreamingContext这个对象的
  * 测试步骤：
  * 1.先正常运行程序一段时间
  * 2.然后停止程序
  * 3.在启动程序再次输入数据
  * 4.查看再次启动后程序运行的结果是否接上了上一次的结果
  */
object WordCount_DriverHA {
  def main(args: Array[String]): Unit = {
    val checkpointDirectory="hdfs://hadoop:9000/sparkstreaming/checkpoint1"
    System.setProperty("HADOOP_USER_NAME","root")
    def functionToCreateContext(): StreamingContext = {
      //获取编程入口StreamingContext
      val sparkConf: SparkConf = new SparkConf().setAppName("WordCount_DriverHA").setMaster("local[2]")
      val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(4))
      //The checkpoint directory has not been set指定目录
      //如果在进行有状态的计算或者driver的HA程序是 必须要进行checkpoint设置
      streamingContext.checkpoint(checkpointDirectory)
      //通过StreamingContext 构建第一个DStream
      val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop", 9999)
      //对DStream进行各种transformation操作
      val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))
      val wordAndDStream: DStream[(String, Int)] = wordsDStream.map((word: String) => (word, 1))
      /**
        * updateFunc: (Seq[V], Option[S]) => Option[S],
        * numPartitions: Int
        * values: Seq[Int]新值
        * state: Option[Int]): Option[Int] 状态值
        * updateFunction:状态更新函数
        */
      def updateFunction(values: Seq[Int], state: Option[Int]): Option[Int] = {
        val new_value: Int = values.sum
        val state_value: Int = state.getOrElse(0)
        Some(new_value + state_value)
      }
      val result: DStream[(String, Int)] = wordAndDStream.updateStateByKey(updateFunction)
      result.print()
      streamingContext.start()
      streamingContext.awaitTermination()
      streamingContext
    }
    /**
      * checkpointDirectory 这个就是checkpoint的目录如果是第一次执行则创建如果
      * 第二次运行就不会再创建对象了这样就可以还原程序之前的状态了
      */
    val context = StreamingContext
      .getOrCreate(checkpointDirectory, functionToCreateContext _)
    context.start()
    context.awaitTermination()
  }
}
