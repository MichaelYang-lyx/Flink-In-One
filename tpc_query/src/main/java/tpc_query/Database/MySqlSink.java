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

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("@@@@@@MySQLSink open@@@@@@");
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(MySQLConnector.MYSQL_URL, MySQLConnector.MYSQL_USER,
                MySQLConnector.MYSQL_PASSWORD);

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

    @Override
    public void close() throws Exception {

        Statement statement = connection.createStatement();

        System.out.println("==-------- Mysql Query Result Q7 whole ---------==");
        ResultSet resultSet4 = statement.executeQuery(Q7.SQLQuery);
        ResultSetPrinter.printResultSet(resultSet4);

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

            preparedStatement.executeUpdate();
        } catch (Exception e) {

            System.err.println("Error while writing to database: " + e.getMessage());
        }
    }
}
