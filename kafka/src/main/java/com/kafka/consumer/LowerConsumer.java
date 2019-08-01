package com.kafka.consumer;

import kafka.api.FetchRequest;
import kafka.api.FetchRequestBuilder;
import kafka.cluster.BrokerEndPoint;
import kafka.javaapi.*;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 根据指定的topic,offset,partition获取数据
 */
public class LowerConsumer {
    public static void main(String[] args) {
        //定义相关参数
        List<String> brokers = new ArrayList<>();//kafka集群
        brokers.add("hadoop");
        //端口号
        int port = 9092;
        //topic主题
        String topic = "first";
        //分区
        int partition = 0;
        //偏移量
        long offset = 2L;
        LowerConsumer lowerConsumer = new LowerConsumer();
        lowerConsumer.getData(brokers,port,topic,partition,offset);


    }

    //找分区领导
    private BrokerEndPoint findLeader(List<String> brokers, int port, String topic, int partition) {
        for (String broker : brokers) {
            //创建获取leader的消费者对象
            SimpleConsumer leader = new SimpleConsumer(broker, port, 1000,
                    1024 , "getLeader");
            TopicMetadataRequest topicMetadataRequest = new TopicMetadataRequest(Collections.singletonList(topic));
            //获取主题元数据返回值
            TopicMetadataResponse metadataResponse = leader.send(topicMetadataRequest);
            List<TopicMetadata> topicsMetadata = metadataResponse.topicsMetadata();
            for (TopicMetadata topicMetadatum : topicsMetadata) {
                List<PartitionMetadata> partitionsMetadata = topicMetadatum.partitionsMetadata();
                for (PartitionMetadata partitionMetadatum : partitionsMetadata) {
                    if (partition==partitionMetadatum.partitionId()) {
                        return partitionMetadatum.leader();
                    }
                }
            }
        }
        return null;
    }

    //获取数据
    private void getData(List<String> brokers, int port, String topic, int partition, long offset) {
        BrokerEndPoint leader = findLeader(brokers, port, topic, partition);
        if (leader == null) {
            return;
        }
        String leaderHost = leader.host();
        SimpleConsumer getData = new SimpleConsumer(leaderHost, port,
                1000, 1024, "getData");
        //创建获取数据对象
        FetchRequest fetchRequest =
                new FetchRequestBuilder().addFetch(topic, partition, offset, 100).build();
        //获取数据的返回值
        FetchResponse fetchResponse = getData.fetch(fetchRequest);
        //解析返回值
        ByteBufferMessageSet messageAndOffsets = fetchResponse.messageSet(topic, partition);
        for (MessageAndOffset messageAndOffset : messageAndOffsets) {
            long offset1 = messageAndOffset.offset();
            ByteBuffer payload = messageAndOffset.message().payload();
            byte[] bytes=new byte[payload.limit()];
            payload.get(bytes);
            System.out.println(offset1+"--------"+new String(bytes));
        }
    }
}
