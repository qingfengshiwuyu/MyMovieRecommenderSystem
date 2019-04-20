package com.atguigu.kafkastreaming;

import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

public class LogProcessor implements Processor<byte[],byte[]> {

    private ProcessorContext context ;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
    }

    @Override
    public void process(byte[] key, byte[] line) {
        String input = new String(line);
        if(input.contains("MOVIE_RATING_PREFIX:")){
            System.out.println("movie rating coming!!!!" + input);
            input = input.split(":")[1].trim();
            context.forward("logProcessor".getBytes(),input.getBytes());
        }
    }

    @Override
    public void punctuate(long l) {

    }

    @Override
    public void close() {

    }
}
