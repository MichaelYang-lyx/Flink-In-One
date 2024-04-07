package tpc_query;

import java.util.Collections;
import java.util.List;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.util.Collector;

import javafx.scene.chart.PieChart.Data;
import tpc_query.Query.*;
import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.TPCSource;
import tpc_query.Database.MySqlConnector;
import tpc_query.Database.MySqlSink;

public class Main {

    private static String generateSql(DataOperation operation) {
        String tableName = operation.getTableName();
        List<String> content = operation.getContentList();

        String placeholders = String.join(", ", Collections.nCopies(content.size(), "?"));
        if (operation.getOperation() == "+") {
            return String.format("INSERT INTO %s VALUES (%s);", tableName, placeholders);
        } else {
            return String.format("DELETE FROM %s WHERE %s;", tableName, placeholders);
        }
    }

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        IQuery query = new Q5();
        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        env.setParallelism(1);

        // 创建一个 DataStream 来接收 TPCSource 的输出
        DataStream<DataOperation> dataSource = env.addSource(new TPCSource());
        dataSource = dataSource.filter((FilterFunction<DataOperation>) data -> query.filter(data));

        // dataSource.process(new ProcessFunction<DataOperation, String>() {
        // @Override
        // public void processElement(DataOperation dataOperation, Context ctx,
        // Collector<String> out)
        // throws Exception {
        // out.collect(generateSql(dataOperation));
        // }
        // }).print();

        // Create the JDBC sink outside the invoke method
        System.out.print("Creating JDBC sink");
        // final SinkFunction<DataOperation> jdbcSink = JdbcSink.sink(
        // "INSERT INTO ? VALUES (?)",
        // (ps, dataOperation) -> {
        // System.out.println("DataOperation: " + dataOperation);
        // ps.setString(1, dataOperation.getTableName());
        // for (int i = 0; i < dataOperation.getContentList().size(); i++) {
        // ps.setString(i + 2, dataOperation.getContentList().get(i));
        // }
        // String sql = generateSql(dataOperation);
        // System.out.println("Executing SQL: " + sql);
        // },
        // new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        // .withUrl(MySqlConnector.MYSQL_URL)
        // .withDriverName("com.mysql.jdbc.Driver")
        // .withUsername(MySqlConnector.MYSQL_USER)
        // .withPassword(MySqlConnector.MYSQL_PASSWORD)
        // .build());

        // // Use the JDBC sink in the invoke method
        // dataSource.addSink(new SinkFunction<DataOperation>() {
        // @Override
        // public void invoke(DataOperation dataOperation, Context context) throws
        // Exception {
        // jdbcSink.invoke(dataOperation, context);
        // }
        // });
        dataSource.addSink(new MySqlSink());

        // 打印所有的输入流
        dataSource.print();

        env.execute("TPC-H Query");
    }
}
