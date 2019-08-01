package com.kafka.intercetor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 拦截器
 */
public class TimeInterceptor implements ProducerInterceptor<String,String> {

    @Override
    public ProducerRecord<String,String> onSend(ProducerRecord record) {
        return new ProducerRecord(record.topic(),record.key(),System.currentTimeMillis()+","+record.value());
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
