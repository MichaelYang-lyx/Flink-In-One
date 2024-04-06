package tpc_query.Table;

import java.util.HashMap;
import java.util.Map;

public class MemoryTable extends Table {
    private Map<Integer, String> dataMap;

    public MemoryTable() {
        dataMap = new HashMap<>();
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
}
