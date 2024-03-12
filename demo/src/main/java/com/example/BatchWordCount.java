package com.example;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.System;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class BatchWordCount {
    public static void main(String[] args) throws Exception {

        String currentDir = System.getProperty("user.dir");
        System.out.println("Current directory: " + currentDir);

        String filename = "input/words.txt";
        try {
            BufferedReader reader = new BufferedReader(new FileReader(filename));
            String line;
            while ((line = reader.readLine()) != null) {
                System.out.println(line);
            }
            reader.close();
        } catch (IOException e) {
            System.err.println("文件 '" + filename + "' 不存在或无法访问。请检查文件路径是否正确。");
        }

        // 1. 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        // 2. 从文件读取数据 按行读取(存储的元素就是每行的文本)
        DataSource<String> lineDS = env.readTextFile("input/words.txt");
        // 3. 转换数据格式

        FlatMapOperator<String, Tuple2<String, Long>> wordAndOne = lineDS
                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
                    @Override
                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
                        String[] words = line.split(" ");
                        for (String word : words) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                });

        // 4. 按照 word 进行分组
        UnsortedGrouping<Tuple2<String, Long>> wordAndOneUG = wordAndOne.groupBy(0);

        // 5. 分组内聚合统计
        AggregateOperator<Tuple2<String, Long>> sum = wordAndOneUG.sum(1);

        // 6. 打印结果
        sum.print();
        System.out.println("输出结果文件路径：" + currentDir + "/output/result.txt");
        sum.writeAsText("output/result.txt").setParallelism(1);

        env.execute("Flink Job");

    }
}