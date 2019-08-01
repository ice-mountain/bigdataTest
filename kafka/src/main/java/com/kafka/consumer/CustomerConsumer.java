package com.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class CustomerConsumer {
    public static void main(String[] args) {
        //配置信息 ProducerConfig
        Properties properties = new Properties();
        //hadoop集群
        properties.put("bootstrap.servers", "hadoop:9092");
        //消费者组id
        properties.put("group.id", "test3");
        //重复消费的配置
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        //是否自动提交offset
        properties.put("enable.auto.commit", "true");
        //自动提交的延时
        properties.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "10000");
        //对象的反序列化
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        //创建消费者对象
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);
        //指定topic
        consumer.subscribe(Arrays.asList("first", "second", "third"));
        //consumer.assign(Collections.singletonList(new TopicPartition("first",0)));
       // consumer.seek(new TopicPartition("first",0),2);
        while (true) {
            ConsumerRecords<String, String> consumerRecords = consumer.poll(1000);
            for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                System.out.println(consumerRecord.topic() +
                        "-----" + consumerRecord.partition() +
                        "-------" + consumerRecord.value());
            }
        }
    }
}
