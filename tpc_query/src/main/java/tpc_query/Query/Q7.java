package tpc_query.Query;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.DataContent.LineItem;
import tpc_query.DataStream.DataContent.Nation;
import tpc_query.DataStream.DataContent.Orders;
import tpc_query.DataStream.DataContent.Region;

public class Q7 implements IQuery, Serializable {

    public boolean filter(DataOperation data) {

        if (data.getTableName().equals("NATION")) {
            Nation nation = (Nation) data.getDataContent();
            return nation.N_NAME.equals("FRANCE") || nation.N_NAME.equals("GERMANY");
        } else if (data.getTableName().equals("LINEITEM")) {
            LineItem lineItem = (LineItem) data.getDataContent();
            return lineItem.L_SHIPDATE.compareTo("1995-01-01") >= 0
                    && lineItem.L_SHIPDATE.compareTo("1996-12-31") <= 0;
        } else if (data.getTableName().equals("REGION")) {
            return false;
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
        addTable(tableMap, "CUSTOMER", false, false, Arrays.asList("ORDERS"), 1, Arrays.asList("NATION2"));
        addTable(tableMap, "SUPPLIER", false, false, Arrays.asList("LINEITEM"), 1, Arrays.asList("NATION1"));
        addTable(tableMap, "NATION1", false, true, Arrays.asList("SUPPLIER"), 0, new ArrayList<>());
        addTable(tableMap, "NATION2", false, true, Arrays.asList("CUSTOMER"), 0, new ArrayList<>());
    }
}
