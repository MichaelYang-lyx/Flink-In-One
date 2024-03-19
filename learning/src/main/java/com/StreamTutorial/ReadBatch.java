package com.StreamTutorial;

import java.util.ArrayList;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReadBatch {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        ArrayList<Event> clicks = new ArrayList<>();
        clicks.add(new Event("Mary", "./home", 1000L));
        clicks.add(new Event("Bob", "./cart", 2000L));
        // 用执行环境的fromCollection 方法进行读取
        DataStream<Event> stream = env.fromCollection(clicks);
        stream.print();
        env.execute();

        // 我们也可以不构建集合，直接将元素列举出来，调用 fromElements 方法进行读取数据
        DataStreamSource<Event> stream2 = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));
        stream2.print();
        env.execute();
    }

}