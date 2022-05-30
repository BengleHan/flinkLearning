package com.flink.kafka;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class StreamingKafkaSource {

    public static void main(String[] args) throws Exception {
        //获取Flink运行环境
        StreamExecutionEnvironment env=StreamExecutionEnvironment.getExecutionEnvironment();
        String topic="testFlink";

        Properties prop=new Properties();
        prop.setProperty("bootstrap.servers","106.75.116.58:9092");
        prop.setProperty("group.id","con1");

        FlinkKafkaConsumer<String> myConsumer=new FlinkKafkaConsumer<String>(topic,new SimpleStringSchema(), prop);
        myConsumer.setStartFromGroupOffsets();//默认消费策略

        DataStreamSource<String> text=env.addSource(myConsumer);
        text.print().setParallelism(1);

        env.execute("Kafka-consumer");

    }
}
