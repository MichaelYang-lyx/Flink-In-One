package com.StreamTutorial;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class TransFunctionUDFTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> clicks = env.fromElements(new Event("Mary", "./home", 1000L),
                new Event("Bob", "./cart", 2000L));
        DataStream<Event> stream = clicks.filter(new KeyWordFilter("home"));
        stream.print();
        env.execute();
    }

    public static class KeyWordFilter implements FilterFunction<Event> {
        private String keyWord;

        KeyWordFilter(String keyWord) {
            this.keyWord = keyWord;
        }

        @Override
        public boolean filter(Event value) throws Exception {
            return value.url.contains(this.keyWord);
        }
    }
}
