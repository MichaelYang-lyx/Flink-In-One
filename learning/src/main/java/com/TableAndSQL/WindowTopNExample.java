package com.TableAndSQL;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

@Deprecated
public class WindowTopNExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 读取数据源，并分配时间戳、生成水位线
        SingleOutputStreamOperator<Event> eventStream = env
                .fromElements(
                        new Event("Alice", "./home", 1000L),
                        new Event("Bob", "./cart", 1000L),
                        new Event("Alice", "./prod?id=1", 25 * 60 * 1000L),
                        new Event("Alice", "./prod?id=4", 55 * 60 * 1000L),
                        new Event("Bob", "./prod?id=5", 3600 * 1000L + 60 * 1000L),
                        new Event("Cary", "./home", 3600 * 1000L + 30 * 60 * 1000L),
                        new Event("Cary", "./prod?id=7", 3600 * 1000L + 59 * 60 * 1000L))
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                }));

        // 创建表环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 将数据流转换成表，并指定时间属性
        Table eventTable = tableEnv.fromDataStream(
                eventStream,
                $("user"),
                $("url"),
                $("timestamp").rowtime().as("ts"));

        tableEnv.toDataStream(eventTable).print();
        
        // 为 方 便 在 SQL 中 引 用 ， 在 环 境 中 注 册 表 EventTable
        tableEnv.createTemporaryView("EventTable", eventTable);

        // 定义子查询，进行窗口聚合，得到包含窗口信息、用户以及访问次数的结果表
        String subQuery = "SELECT window_start, window_end, user, COUNT(url) as cnt " +
                "FROM TABLE ( " +
                "TUMBLE( TABLE EventTable, DESCRIPTOR(ts), INTERVAL '1' HOUR )) " +
                "GROUP BY window_start, window_end, user ";

        
        // 定义 Top N 的外层查询
        String topNQuery = "SELECT * " +
                "FROM (" +
                "SELECT *, " +
                "ROW_NUMBER() OVER ( " +
                "PARTITION BY window_start, window_end " +
                "ORDER BY cnt desc " +
                ") AS row_num " +
                "FROM (" + subQuery + ")) " +
                "WHERE row_num <= 2";

        // 执行 SQL 得到结果表
        Table result = tableEnv.sqlQuery(topNQuery);
        tableEnv.toDataStream(result).print();
        env.execute();
    }
}
