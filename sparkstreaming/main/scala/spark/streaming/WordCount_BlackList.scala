package spark.streaming

import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, ReceiverInputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object WordCount_BlackList {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME","root")
    val checkpointDir="hdfs://hadoop:9000/sparkstreaming/checkpoint02"
    val sparkConf: SparkConf = new SparkConf().setAppName("WordCount_BlackList").setMaster("local[2]")
    val streamingContext: StreamingContext = new StreamingContext(sparkConf,Seconds(4))
    streamingContext.checkpoint(checkpointDir)
    val blackList=List(".","!",",","@","$","%","^","*")
    val inputDStream: ReceiverInputDStream[String] = streamingContext.socketTextStream("hadoop",9999)
    val wordsDStream: DStream[String] = inputDStream.flatMap(_.split(" "))
    val bc: Broadcast[List[String]] = streamingContext.sparkContext.broadcast(blackList)
    val transformFunc:RDD[String]=>RDD[String]=(rdd:RDD[String])=>{
      val newRDD: RDD[String] = rdd.mapPartitions(ptndata => {
        val bl: List[String] = bc.value
        val newPtn: Iterator[String] = ptndata.filter(x => !bl.contains(x))
        newPtn
      })
      newRDD
    }
    //transform 的作用就是把rdd中的每个数据拿出来处理一次
    val trueWordsDStream: DStream[String] = wordsDStream.transform(transformFunc)
    val result: DStream[(String, Int)] = trueWordsDStream.map(x => (x, 1)).updateStateByKey((values: Seq[Int], state: Option[Int]) => {
      Some(values.sum + state.getOrElse(0))
    })
    result.print()
    streamingContext.start()
    streamingContext.awaitTermination()
  }
}
