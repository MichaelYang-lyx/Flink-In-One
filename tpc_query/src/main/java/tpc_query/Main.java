package tpc_query;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import tpc_query.Query.*;
import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.TPCSource;
import tpc_query.Database.MemorySink;
import tpc_query.Database.MySQLConnector;
import tpc_query.Database.MySQLSink;

public class Main {

    public static void main(String[] args) throws Exception {

        Runtime runtime = Runtime.getRuntime();
        runtime.gc();
        long memoryBefore = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Memory usage before execution: " + memoryBefore + " bytes");

        long startTime = System.currentTimeMillis();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        IQuery query = new Q7();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        DataStream<DataOperation> dataSource = env.addSource(new TPCSource());
        dataSource = dataSource.filter((FilterFunction<DataOperation>) data -> query.filter(data));
        dataSource = dataSource.keyBy((KeySelector<DataOperation, String>) DataOperation::getKey);
        // Create the JDBC sink outside the invoke method

        // System.out.print("Creating JDBC sink");
        MySQLConnector.clearTPCHData();

        /* 选 MySQL还是 Memory */
        String choice = "Memory";
        if (choice.equals("MySQL")) {
            MySQLSink mySQLSink = new MySQLSink();
            dataSource.addSink(mySQLSink);
        } else {

            MemorySink memorySink = new MemorySink();
            dataSource.addSink(memorySink);
        }

        env.execute("TPC-H Query");
        System.out.println("The Program is Over!");
        long endTime = System.currentTimeMillis();
        long executionTime = endTime - startTime;
        System.out.println("Query executed in " + executionTime / 1000.0 + " seconds.");

        long memoryAfter = runtime.totalMemory() - runtime.freeMemory();
        System.out.println("Memory usage after execution: " + memoryAfter + " bytes");
        long memoryUsed = memoryAfter - memoryBefore;
        System.out.println("Memory used by execution: " + memoryUsed + " bytes");

    }
}
