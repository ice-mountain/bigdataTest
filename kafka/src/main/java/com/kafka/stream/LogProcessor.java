package com.kafka.stream;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[],byte[]> {
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext processorContext) {
        context=processorContext;

    }
    @Override
    public void process(byte[] bytes, byte[] bytes2) {
        //获取一个条数据
        String line = new String(bytes2);
        line=line.replaceAll(">>>","");
        bytes2 = line.getBytes();
        context.forward(bytes,bytes2);
    }
    @Override
    public void punctuate(long l) {

    }
    @Override
    public void close() {

    }
}
