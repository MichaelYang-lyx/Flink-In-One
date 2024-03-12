package com.StreamTutorial;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class RebalanceTest {
    public static void main(String[] args) throws Exception {
        // 创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        // 读取数据源，并行度为 1
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        // 经 轮 询 重 分 区 后 打 印 输 出 ， 并 行 度 为 4
        stream.rebalance().print("rebalance").setParallelism(4);
        env.execute();
    }
}
