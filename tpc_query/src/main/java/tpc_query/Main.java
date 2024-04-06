package tpc_query;

import java.util.ArrayList;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tpc_query.Query.*;
import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.TPCSource;

public class Main {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        IQuery query = new Q5();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        // 创建一个 DataStream 来接收 TPCSource 的输出
        DataStream<DataOperation> dataSource = env.addSource(new TPCSource());
        dataSource = dataSource.filter((FilterFunction<DataOperation>) data -> query.filter(data));

        // 打印所有的输入流
        dataSource.print();

        env.execute("TPC-H Query");
    }


    
}
