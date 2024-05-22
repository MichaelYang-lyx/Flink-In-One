package tpc_query.Database;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.Query.IQuery;
import tpc_query.Query.Q7;
import tpc_query.Update.Delete;
import tpc_query.Update.Insert;
import java.io.File;

public class MySQLSink extends RichSinkFunction<DataOperation> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    private MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState;
    public TableController tableController;

    public Map<String, ITable> tables;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(MySQLConnector.MYSQL_URL, MySQLConnector.MYSQL_USER,
                MySQLConnector.MYSQL_PASSWORD);
        joinResultState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("ResultState", Types.LONG,
                        Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE)));

        tableController = new TableController("MySQL");
        IQuery query = new Q7();
        tableController.setupTables(query);
        tables = tableController.tables;

    }

    String queryq7 = "SELECT " +
            "n1.n_name AS supp_nation, " +
            "n2.n_name AS cust_nation, " +
            "EXTRACT(YEAR FROM l_shipdate) AS l_year, " +
            "l_extendedprice * (1 - l_discount) AS volume " +
            "FROM supplier, lineitem, orders, customer, nation n1, nation n2 " +
            "WHERE s_suppkey = l_suppkey " +
            "AND o_orderkey = l_orderkey " +
            "AND c_custkey = o_custkey " +
            "AND s_nationkey = n1.n_nationkey " +
            "AND c_nationkey = n2.n_nationkey " +
            "AND ( (n1.n_name = 'FRANCE' AND n2.n_name = 'GERMANY') " +
            "OR (n1.n_name = 'GERMANY' AND n2.n_name = 'FRANCE') ) " +
            "AND l_shipdate BETWEEN DATE '1995-01-01' AND DATE '1996-12-31'";

    String query_lineitem = "SELECT * FROM lineitem";
    String query_nation = "SELECT * FROM nation";
    String test_query = "SELECT " +
            "n1.n_name AS supp_nation, " +
            "n2.n_name AS cust_nation, " +
            "EXTRACT(YEAR FROM l_shipdate) AS l_year, " +
            "l_extendedprice * (1 - l_discount) AS volume " +
            "FROM supplier, lineitem, orders, customer, nation n1, nation n2 " +
            "WHERE s_suppkey = l_suppkey " +
            "AND o_orderkey = l_orderkey " +
            "AND c_custkey = o_custkey " +
            "AND s_nationkey = n1.n_nationkey " +
            "AND c_nationkey = n2.n_nationkey ";

    @Override
    public void close() throws Exception {

        try {
            for (Map.Entry<String, ITable> entry : tables.entrySet()) {
                MySQLTable table = (MySQLTable) entry.getValue();
                PrintWriter writer = new PrintWriter(new File("output/tpc_query/" + entry.getKey() + ".txt"));
                writer.println("-------- Table Name: " + entry.getKey() + "---------");
                for (Map.Entry<Long, IDataContent> tupleEntry : table.allTuples.entrySet()) {
                    writer.println("Primary Key: " + tupleEntry.getKey());
                    writer.println("Data Content: " + tupleEntry.getValue());
                }
                writer.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        Statement statement = connection.createStatement();

        // System.out.println("==-------- Mysql Query Result Lineitem ---------==");
        // ResultSet resultSet = statement.executeQuery(query_lineitem);
        // ResultSetPrinter.printResultSet(resultSet);

        System.out.println("==-------- Mysql Query Result Nation ---------==");
        ResultSet resultSet2 = statement.executeQuery(query_nation);
        ResultSetPrinter.printResultSet(resultSet2);

        System.out.println("==-------- Mysql Query Result Q7 Part ---------==");
        ResultSet resultSet3 = statement.executeQuery(test_query);
        ResultSetPrinter.printResultSet(resultSet3);

        System.out.println("==-------- Mysql Query Result Q7 whole ---------==");
        ResultSet resultSet4 = statement.executeQuery(queryq7);
        ResultSetPrinter.printResultSet(resultSet4);

        System.out.println("==-------- Q7.directSelect1 ---------==");
        System.out.println(Q7.directSelect1(tables));

        System.out.println("==-------- joinResult) ---------==");
        Iterable<Map.Entry<Long, Tuple4<String, String, Integer, Double>>> entries = joinResultState.entries();
        for (Map.Entry<Long, Tuple4<String, String, Integer, Double>> entry : entries) {
            Long key = entry.getKey();
            Tuple4<String, String, Integer, Double> value = entry.getValue();
            System.out.println("Key: " + key + ", Value: " + value);
        }

        super.close();
        if (preparedStatement != null) {
            preparedStatement.close();
        }
        if (connection != null) {
            connection.close();
        }
        super.close();
    }

    private static String generateSql(DataOperation operation) {

        String tableName = operation.getTableName();

        List<String> content = operation.getContentList();
        if (operation.getOperation().equals("+")) {
            String placeholders = String.join(", ", Collections.nCopies(content.size(), "?"));
            return String.format("INSERT INTO %s VALUES (%s);", tableName, placeholders);
        } else {
            return String.format("DELETE FROM %s WHERE %s;", tableName, operation.dataContent.primaryKeySQL());
        }
    }

    @Override
    public void invoke(DataOperation dataOperation, Context context) throws Exception {
        // below for test
        try {
            String sql = generateSql(dataOperation);
            preparedStatement = connection.prepareStatement(sql);
            if (dataOperation.operation.equals("+")) {
                for (int i = 0; i < dataOperation.getContentList().size(); i++) {
                    preparedStatement.setString(i + 1, dataOperation.getContentList().get(i));
                }
            }

            // System.out.println("** " + dataOperation.operation + " " +
            // preparedStatement);
            preparedStatement.executeUpdate();
        } catch (Exception e) {

            System.err.println("Error while writing to database: " + e.getMessage());
        }
        // 这里还需要添加维护relationship的操作
        if (dataOperation.operation.equals("+")) {

            if (dataOperation.tableName.equals("NATION")) {
                dataOperation.switchTableName("NATION1");
                Insert.insert(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
                dataOperation.switchTableName("NATION2");
                Insert.insert(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
                dataOperation.switchTableName("NATION");

            } else {
                Insert.insert(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
            }
        } else {
            if (dataOperation.tableName.equals("NATION")) {
                dataOperation.switchTableName("NATION1");
                Delete.delete(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
                dataOperation.switchTableName("NATION2");
                Delete.delete(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
                dataOperation.switchTableName("NATION");

            } else {
                Delete.delete(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
            }

        }

    }
}
