package tpc_query.Table;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import tpc_query.Query.IQuery;
import tpc_query.Query.Q5;
import org.apache.flink.api.java.tuple.Tuple5;

public class TableController {

    Map<String, Tuple5<Boolean, Boolean, String, Integer, List<String>>> tableMap;

    public TableController() {
        this.tableMap = new HashMap<>();
    }

    public void setupTables(IQuery query) {
        query.tableInitialization(this.tableMap);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TableController {\n");
        for (Map.Entry<String, Tuple5<Boolean, Boolean, String, Integer, List<String>>> entry : tableMap.entrySet()) {
            String tableName = entry.getKey();
            Tuple5<Boolean, Boolean, String, Integer, List<String>> tableInfo = entry.getValue();
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
