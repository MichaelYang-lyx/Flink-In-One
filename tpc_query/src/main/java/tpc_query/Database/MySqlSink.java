package tpc_query.Database;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;

import tpc_query.DataStream.DataOperation;

public class MySqlSink extends RichSinkFunction<DataOperation> {
    private Connection connection;
    private PreparedStatement preparedStatement;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connection = DriverManager.getConnection(MySqlConnector.MYSQL_URL, MySqlConnector.MYSQL_USER,
                MySqlConnector.MYSQL_PASSWORD);

    }

    @Override
    public void close() throws Exception {
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

        String placeholders = String.join(", ", Collections.nCopies(content.size(), "?"));
        System.out.println(operation.getOperation());
        if (operation.getOperation().equals("+")) {
            return String.format("INSERT INTO %s VALUES (%s);", tableName, placeholders);
        } else {
            return String.format("DELETE FROM %s WHERE %s;", tableName, placeholders);
        }
    }

    @Override
    public void invoke(DataOperation dataOperation, Context context) throws Exception {
        try {
            String sql = generateSql(dataOperation);
            System.out.println(sql);
            preparedStatement = connection.prepareStatement(sql);
            for (int i = 0; i < dataOperation.getContentList().size(); i++) {
                preparedStatement.setString(i + 1, dataOperation.getContentList().get(i));
            }
            preparedStatement.executeUpdate();
        } catch (Exception e) {
            System.err.println("Error while writing to database: " + e.getMessage());
        }
    }
}
