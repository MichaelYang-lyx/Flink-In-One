package com.StreamTutorial;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransKeyByTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 2000L));
        clicks.add(new Event("Bob", "./cart2", 3000L));
        // 用执行环境的fromCollection 方法进行读取
        DataStream<Event> stream = env.fromCollection(clicks);
        System.out.println("Java 版本：" + System.getProperty("java.version"));
        // 使用Lambda表达式

        KeyedStream<Event, String> keyedStream = stream.keyBy(e -> e.user);
        stream.print();
        keyedStream.print();

        env.execute();

        // 使用lambda表达式遍历列表
        // numbers.forEach(number -> System.out.println(number));
    }

}
