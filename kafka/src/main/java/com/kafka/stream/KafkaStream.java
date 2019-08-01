package com.kafka.stream;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class KafkaStream {
    public static void main(String[] args) {
        //创建topology对象
        TopologyBuilder builder = new TopologyBuilder();
        //创建配置文件
        Properties properties = new Properties();
        properties.put("bootstrap.servers","hadoop:9092");
        properties.put("application.id","kafkaStream");
        //构建拓扑结构
        builder.addSource("SOURCE","first")
                .addProcessor("PROCESS", () -> new LogProcessor(), "SOURCE")
                .addSink("SINK","first","PROCESS");
        KafkaStreams kafkaStreams = new KafkaStreams(builder, properties);
        kafkaStreams.start();
    }
}
