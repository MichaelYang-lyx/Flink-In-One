package tpc_query.Database;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

public class MemoryTable extends Table {
    private Map<Integer, String> dataMap;

    public MemoryTable() {
        dataMap = new HashMap<>();
    }

    public MemoryTable(String tableName, Tuple5<Boolean, Boolean, String, Integer, List<String>> info) {
        dataMap = new HashMap<>();
        this.tableName = tableName;
        this.isRoot = info.f0;
        this.isLeaf = info.f1;
        this.parent = info.f2;
        this.numChild = info.f3;
        this.childs = info.f4;
    }

    @Override
    public void insertRow(String data) {
        // Implement memory-based insert logic
        // For example, add data to the dataMap
    }

    @Override
    public String getRow(int id) {
        // Implement memory-based select logic
        return dataMap.get(id);
    }

    @Override
    public void updateRow(int id, String newData) {
        // Implement memory-based update logic
        dataMap.put(id, newData);
    }

    @Override
    public void deleteRow(int id) {
        // Implement memory-based delete logic
        dataMap.remove(id);
    }

    @Override
    public String toString() {
        return "MemoryTable{" +
                "tableName='" + tableName + '\'' +
                ", isRoot=" + isRoot +
                ", isLeaf=" + isLeaf +
                ", parent='" + parent + '\'' +
                ", numChild=" + numChild +
                ", childs=" + childs +
                ", dataMap=" + dataMap +
                '}';
    }

}
