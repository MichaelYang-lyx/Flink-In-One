package tpc_query.Table;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

public class TableController {

    Map<String, Tuple5<Boolean, Boolean, String, Integer, List<String>>> tableMap;

    public TableController() {
        this.tableMap = new HashMap<>();
    }

    private void initializeTable(String tableName, Boolean isRoot, Boolean isLeaf, String parent, int numChild,
            List<String> childs) {
        this.tableMap.put(tableName, new Tuple5<>(isRoot, isLeaf, parent, numChild, childs));
    }

    public void setupTables() {
        initializeTable("LineItme", true, false, null, 2, Arrays.asList("Orders", "Supplier"));
        initializeTable("Orders", false, false, "LineItme", 1, Arrays.asList("Customer"));
        initializeTable("Customer", false, false, "Orders", 1, Arrays.asList("Nation2"));
        initializeTable("Supplier", false, false, "Lineitem", 1, Arrays.asList("Nation1"));
        initializeTable("Nation1", false, true, "Supplier", 0, new ArrayList<>());
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
        controller.setupTables();
        System.out.println(controller);
    }
}
