package spark.streaming

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object FileWordCount {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    //获取编程入口StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("FileWordCount").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf, Seconds(4))
    //The checkpoint directory has not been set指定目录
    streamingContext.checkpoint("hdfs://hadoop:9000/sparkstreaming/checkpoint")
    //通过StreamingContext 构建第一个DStream
    //这里的参数只能是目录并且这里的inputDStream 结果必须是新添加的文件才会有结果
    val inputDStream: DStream[String] = streamingContext.textFileStream("hdfs://hadoop:9000/student")
    //对DStream进行各种transformation操作
    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(","))
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
  }
}
