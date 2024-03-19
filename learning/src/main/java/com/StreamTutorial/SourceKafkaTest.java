package com.StreamTutorial;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import java.util.Properties;

@SuppressWarnings("deprecation")
public class SourceKafkaTest {
        public static void main(String[] args) throws Exception {
                StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
                env.setParallelism(1);
                Properties properties = new Properties();
                properties.setProperty("bootstrap.servers", "localhost:9092");
                properties.setProperty("group.id", "consumer-group");
                properties.setProperty("key.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer");
                properties.setProperty("value.deserializer",
                                "org.apache.kafka.common.serialization.StringDeserializer");
                properties.setProperty("auto.offset.reset", "latest");
                System.out.println("begin");
                System.out.println(properties);
                DataStreamSource<String> stream = env.addSource(new FlinkKafkaConsumer<String>("clicks",
                                new SimpleStringSchema(),
                                properties));
                stream.print("Kafka");
                env.execute();
        }
}