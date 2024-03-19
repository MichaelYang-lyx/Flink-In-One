package com.StreamTutorial;

import java.util.Random;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

@SuppressWarnings("deprecation")
public class ParallelSourceExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(new CustomSource()).setParallelism(2).print();
        env.execute();
    }

    public static class CustomSource implements ParallelSourceFunction<Integer> {
        private boolean running = true;
        private Random random = new Random();
        private long startTime = System.currentTimeMillis();

        @Override
        public void run(SourceContext<Integer> sourceContext) throws Exception {
            while (running && (System.currentTimeMillis() - startTime) < 3000) {
                sourceContext.collect(random.nextInt());
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

}