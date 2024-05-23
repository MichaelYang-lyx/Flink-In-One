package tpc_query.Database;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import tpc_query.Query.IQuery;
import tpc_query.Query.Q5;
import org.apache.flink.api.java.tuple.Tuple5;
import tpc_query.Database.MySQLConnector;

public class TableController {

    Map<String, Tuple5<Boolean, Boolean, List<String>, Integer, List<String>>> tableInfo;
    public Map<String, ITable> tables; // just for memory
    String type;

    public TableController() {
        this.tableInfo = new HashMap<>();
        this.type = "memory";
    }

    public TableController(String type) {
        this.tableInfo = new HashMap<>();
        this.type = type;

    }

    public void setupTables(IQuery query) {
        query.registerTables(this.tableInfo);
        if (this.type.equals("Memory")) {
            this.tables = new HashMap<>();
            for (Map.Entry<String, Tuple5<Boolean, Boolean, List<String>, Integer, List<String>>> entry : tableInfo
                    .entrySet()) {
                String tableName = entry.getKey();
                ITable table = new MyTable(tableName, entry.getValue()); // temp is the same thing
                this.tables.put(tableName, table);
            }
        } else if (this.type.equals("MySQL")) {
            this.tables = new HashMap<>();
            for (Map.Entry<String, Tuple5<Boolean, Boolean, List<String>, Integer, List<String>>> entry : tableInfo
                    .entrySet()) {
                String tableName = entry.getKey();
                ITable table = new MyTable(tableName, entry.getValue());
                this.tables.put(tableName, table);
            }
            /*
             * try {
             * MySQLConnector.createTPCHTable();
             * MySQLConnector.clearTPCHData();
             * } catch (ClassNotFoundException e) {
             * e.printStackTrace();
             * } catch (SQLException e) {
             * e.printStackTrace();
             * }
             */

        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TableController {\n");
        for (Map.Entry<String, Tuple5<Boolean, Boolean, List<String>, Integer, List<String>>> entry : tableInfo
                .entrySet()) {
            String tableName = entry.getKey();
            Tuple5<Boolean, Boolean, List<String>, Integer, List<String>> tableInfo = entry.getValue();
            sb.append("  Table: ").append(tableName);
            sb.append("\n    Is Root: ").append(tableInfo.f0);
            sb.append("\n    Is Leaf: ").append(tableInfo.f1);
            sb.append("\n    Parent Table: ").append(tableInfo.f2);
            sb.append("\n    Number of Children: ").append(tableInfo.f3);
            sb.append("\n    Child Tables: ").append(tableInfo.f4);
            sb.append("\n\n");
        }
        sb.append("}");
        return sb.toString();
    }

    public static void main(String[] args) {
        TableController controller = new TableController();
        IQuery query = new Q5();
        controller.setupTables(query);
        System.out.println(controller);
    }
}
