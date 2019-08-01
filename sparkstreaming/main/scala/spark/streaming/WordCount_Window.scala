package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}

object WordCount_Window {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    //获取编程入口StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("NewWordCount").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(2))
    //The checkpoint directory has not been set指定目录
    streamingContext.checkpoint("hdfs://hadoop:9000/sparkstreaming/checkpoint3")
    //通过StreamingContext 构建第一个DStream
    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop", 9999)
    //对DStream进行各种transformation操作
    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val wordAndDStream: DStream[(String, Int)] = wordsDStream.map((word: String) => (word, 1))
    val result: DStream[(String, Int)] = wordAndDStream
      .reduceByKeyAndWindow((x:Int, y:Int)=>(x+y),Seconds(4),Seconds(6))
    result.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
