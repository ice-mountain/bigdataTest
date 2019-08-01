package com.kafka.producer;

import org.apache.kafka.clients.producer.Partitioner;
import org.apache.kafka.common.Cluster;

import java.util.Map;

/**
 * 自定义分区
 */
public class CustomerPartition implements Partitioner {

    public int partition(String topic, Object key, byte[] keyBytes, Object value, byte[] valueBytes, Cluster cluster) {
        return 0;
    }

    public void close() {

    }

    public void configure(Map<String, ?> configs) {

    }
}
