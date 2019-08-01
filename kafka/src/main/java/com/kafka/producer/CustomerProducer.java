package com.kafka.producer;

import org.apache.kafka.clients.producer.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * 低级的API的消息提供者
 */
public class CustomerProducer {
    public static void main(String[] args) {
        //配置信息 ProducerConfig
        Properties properties = new Properties();
        //hadoop集群
        properties.put("bootstrap.servers", "hadoop:9092");
        //应答级别
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        //重试次数
        properties.put("retries", 0);
        //批量大小
        properties.put("batch.size", 16384);
        //提交延时
        properties.put("linger.ms", 1);
        //缓冲内存
        properties.put("buffer.memory", 33554432);
        //提供序列化的类
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, CustomerPartition.class);
        List<String> list = new ArrayList<>();
        list.add("com.kafka.intercetor.TimeInterceptor");
        list.add("com.kafka.intercetor.CountInterceptor");
        properties.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG,list);
        //创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        //循环发送数据 回调函数
        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("first", String.valueOf(i)),
                    (metadata, exception) -> {
                        if (exception != null) {
                            System.out.println("发送失败");
                        } else {
                            System.out.println(metadata.partition() + "---" + metadata.offset());
                        }
                    });
        }
        producer.close();
    }
}
