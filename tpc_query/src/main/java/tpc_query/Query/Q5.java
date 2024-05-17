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

    private void addTable(Map<String, Tuple5<Boolean, Boolean, List<String>, Integer, List<String>>> tableMap,
            String tableName, Boolean isRoot, Boolean isLeaf, List<String> parent, int numChild,
            List<String> childs) {
        tableMap.put(tableName, new Tuple5<>(isRoot, isLeaf, parent, numChild, childs));
    }

    public void registerTables(Map<String, Tuple5<Boolean, Boolean, List<String>, Integer, List<String>>> tableMap) {
        addTable(tableMap, "LINEITEM", true, false, null, 2, Arrays.asList("ORDERS", "SUPPLIER"));
        addTable(tableMap, "ORDERS", false, false, Arrays.asList("LINEITEM"), 1, Arrays.asList("CUSTOMER"));
        addTable(tableMap, "CUSTOMER", false, false, Arrays.asList("ORDERS"), 1, Arrays.asList("SUPPLIER"));
        addTable(tableMap, "SUPPLIER", false, false, Arrays.asList("LINEITEM", "CUSTOMER"), 1, Arrays.asList("NATION"));
        addTable(tableMap, "NATION", false, true, Arrays.asList("SUPPLIER"), 1, Arrays.asList("REGION"));
        addTable(tableMap, "REGION", false, true, Arrays.asList("NATION"), 0, new ArrayList<>());
    }
}
