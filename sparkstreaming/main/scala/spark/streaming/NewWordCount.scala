package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object NewWordCount {
  def main(args: Array[String]): Unit = {
    //获取编程入口StreamingContext
    val sparkConf: SparkConf = new SparkConf().setAppName("NewWordCount").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(4))
    //通过StreamingContext 构建第一个DStream
    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop",9999)
    //对DStream进行各种transformation操作
    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val wordAndDStream: DStream[(String, Int)] = wordsDStream.map((word: String) =>(word,1))
    val result: DStream[(String, Int)] = wordAndDStream.reduceByKey((x, y)=>x+y)
    //对于结果数据进行output操作
    result.print()
    //提交sparkStreaming的应用程序StreamingContext.start()  ssc.awaitTermination()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
