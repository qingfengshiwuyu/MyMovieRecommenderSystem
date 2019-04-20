package com.atguigu.kafkastreaming;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.TopologyBuilder;

import java.util.Properties;

public class Application {

    public static void main(String[] args) {
        String brokers = "hadoop102:9092";
        String zookeepers = "hadoop102:2181";

        //定义输入和输出的topic
        String from = "log";
        String to = "recommender";

        //定义kafka的配置
        Properties settings = new Properties();
        settings.put(StreamsConfig.APPLICATION_ID_CONFIG,"logFilter");
        settings.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,brokers);
        settings.put(StreamsConfig.ZOOKEEPER_CONNECT_CONFIG,zookeepers);

        StreamsConfig config = new StreamsConfig(settings);
        //创建拓扑对象
        TopologyBuilder builder = new TopologyBuilder();
        //建立连接
        builder.addSink("SOURCE",from)
                .addProcessor("PROCESSOR",() ->new LogProcessor(),"SOURCE")
                .addSink("SINK",to,"PROCESSOR");

        KafkaStreams kafkaStreams = new KafkaStreams(builder, config);
        kafkaStreams.start();

    }
}
