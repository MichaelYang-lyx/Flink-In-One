package tpc_query;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import tpc_query.Query.*;
import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.TPCSource;
import tpc_query.Database.MemoryTable;
import tpc_query.Database.MySQLConnector;
import tpc_query.Database.MySQLSink;
import tpc_query.Database.MySQLTable;
import tpc_query.Database.TableController;

import tpc_query.Update.Insert;

public class Main {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        IQuery query = new Q7();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        // 创建一个 DataStream 来接收 TPCSource 的输出
        DataStream<DataOperation> dataSource = env.addSource(new TPCSource());
        dataSource = dataSource.filter((FilterFunction<DataOperation>) data -> query.filter(data));
        dataSource = dataSource.keyBy((KeySelector<DataOperation, String>) DataOperation::getKey);
        // Create the JDBC sink outside the invoke method

        // System.out.print("Creating JDBC sink");
        MySQLConnector.clearTPCHData();
        dataSource.addSink(new MySQLSink()); // 要用

        env.execute("TPC-H Query");
    }
}
