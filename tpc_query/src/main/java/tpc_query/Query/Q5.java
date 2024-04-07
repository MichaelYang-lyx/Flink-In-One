package tpc_query.Query;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.DataContent.Orders;
import tpc_query.DataStream.DataContent.Region;

public class Q5 implements IQuery, Serializable {

    public boolean filter(DataOperation data) {
        if (data.getTableName().equals("region.tbl")) {
            Region region = (Region) data.getDataContent();
            return region.getR_NAME().equals("ASIA");
        } else if (data.getTableName().equals("orders.tbl")) {
            Orders orders = (Orders) data.getDataContent();
            return orders.getO_ORDERDATE().compareTo("1994-01-01") >= 0
                    && orders.getO_ORDERDATE().compareTo("1995-01-01") <= 0;
        }
        return true;
    }

    private void addTable(Map<String, Tuple5<Boolean, Boolean, String, Integer, List<String>>> tableMap,
            String tableName, Boolean isRoot, Boolean isLeaf, String parent, int numChild,
            List<String> childs, Object newParam) {
        tableMap.put(tableName, new Tuple5<>(isRoot, isLeaf, parent, numChild, childs));
    }

    public void tableInitialization(Map<String, Tuple5<Boolean, Boolean, String, Integer, List<String>>> tableMap) {
        addTable(tableMap, "LineItme", true, false, null, 2, Arrays.asList("Orders", "Supplier"), null);
        addTable(tableMap, "Orders", false, false, "LineItme", 1, Arrays.asList("Customer"), null);
        addTable(tableMap, "Customer", false, false, "Orders", 1, Arrays.asList("Nation2"), null);
        addTable(tableMap, "Supplier", false, false, "Lineitem", 1, Arrays.asList("Nation1"), null);
        addTable(tableMap, "Nation1", false, true, "Supplier", 0, new ArrayList<>(), null);
    }

}
