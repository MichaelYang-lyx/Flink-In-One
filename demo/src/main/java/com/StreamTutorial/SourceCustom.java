package com.StreamTutorial;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

@SuppressWarnings("deprecation")
public class SourceCustom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 有了自定义的 source function，调用 addSource 方法
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.print("SourceCustom");
        env.execute();
    }
}
