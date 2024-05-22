package tpc_query.Query;

import java.io.Serializable;
import java.sql.Date;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Hashtable;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;

import com.ibm.icu.util.Calendar;

import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.DataContent.Customer;
import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.DataStream.DataContent.LineItem;
import tpc_query.DataStream.DataContent.Nation;
import tpc_query.DataStream.DataContent.Orders;
import tpc_query.DataStream.DataContent.Region;
import tpc_query.DataStream.DataContent.Supplier;
import tpc_query.Database.ITable;
import tpc_query.Database.MySQLTable;

public class Q7 implements IQuery, Serializable {

    public boolean filter(DataOperation data) {

        if (data.getTableName().equals("NATION")) {
            Nation nation = (Nation) data.getDataContent();
            return nation.N_NAME.equals("FRANCE") || nation.N_NAME.equals("GERMANY");
        } else if (data.getTableName().equals("LINEITEM")) {
            LineItem lineItem = (LineItem) data.getDataContent();
            Date startDate = Date.valueOf("1995-01-01");
            Date endDate = Date.valueOf("1996-12-31");
            return lineItem.L_SHIPDATE.compareTo(startDate) >= 0
                    && lineItem.L_SHIPDATE.compareTo(endDate) <= 0;
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

    public static List<Tuple4<String, String, Integer, Double>> directSelect1(Map<String, ITable> tables)
            throws Exception {

        MySQLTable lineItemTable = (MySQLTable) tables.get("LINEITEM");
        MySQLTable supplierTable = (MySQLTable) tables.get("SUPPLIER");
        MySQLTable customerTable = (MySQLTable) tables.get("CUSTOMER");
        MySQLTable ordersTable = (MySQLTable) tables.get("ORDERS");
        MySQLTable nation1Table = (MySQLTable) tables.get("NATION1");
        MySQLTable nation2Table = (MySQLTable) tables.get("NATION2");

        List<Tuple4<String, String, Integer, Double>> result = new ArrayList<>();

        // Fetch all tuples

        Hashtable<Long, IDataContent> lineItems = lineItemTable.allTuples;
        Hashtable<Long, IDataContent> suppliers = supplierTable.allTuples;
        Hashtable<Long, IDataContent> customers = customerTable.allTuples;
        Hashtable<Long, IDataContent> all_orders = ordersTable.allTuples;
        Hashtable<Long, IDataContent> nations1 = nation1Table.allTuples;
        Hashtable<Long, IDataContent> nations2 = nation2Table.allTuples;

        // No need for filtering as filtering has been finished in datasource sink
        for (IDataContent lineItemContent : lineItems.values()) {
            LineItem lineItem = (LineItem) lineItemContent;
            Supplier supplier = (Supplier) suppliers.get((long) lineItem.L_SUPPKEY);
            if (supplier == null || !nations1.containsKey(supplier.S_NATIONKEY))
                continue;

            Orders orders = (Orders) all_orders.get((long) lineItem.L_ORDERKEY);
            if (orders == null)
                continue;

            Customer customer = (Customer) customers.get((long) orders.O_CUSTKEY);
            if (customer == null || !nations2.containsKey(customer.C_NATIONKEY))
                continue;

            Nation nation1 = (Nation) nations1.get((long) supplier.S_NATIONKEY);
            Nation nation2 = (Nation) nations2.get((long) customer.C_NATIONKEY);

            String suppNation = nation1.N_NAME;
            String custNation = nation2.N_NAME;
            if (!((suppNation.equals("FRANCE") && custNation.equals("GERMANY"))
                    || (suppNation.equals("GERMANY") && custNation.equals("FRANCE")))) {
                continue;
            }

            Date l_shipdate = lineItem.L_SHIPDATE;
            Calendar calendar = Calendar.getInstance();
            calendar.setTime(l_shipdate);
            int l_shipYear = calendar.get(Calendar.YEAR);
            double volume = lineItem.L_EXTENDEDPRICE * (1 - lineItem.L_DISCOUNT);
            result.add(new Tuple4<>(suppNation, custNation, l_shipYear, volume));
        }

        return result;
    }

    public static Tuple4<String, String, Integer, Double> selectResult(Map<String, ITable> tables,
            Long lineitemPKey) throws Exception {
        // Get Table
        MySQLTable lineItem_Table = (MySQLTable) tables.get("LINEITEM");
        MySQLTable supplier_Table = (MySQLTable) tables.get("SUPPLIER");
        MySQLTable customer_Table = (MySQLTable) tables.get("CUSTOMER");
        MySQLTable orders_Table = (MySQLTable) tables.get("ORDERS");
        MySQLTable nation1_Table = (MySQLTable) tables.get("NATION1");
        MySQLTable nation2_Table = (MySQLTable) tables.get("NATION2");
        // Get Data
        LineItem lineItem = (LineItem) lineItem_Table.indexLiveTuple.get(lineitemPKey);
        Long l_orderkey = lineItem.L_ORDERKEY;
        Long l_suppkey = lineItem.L_SUPPKEY;
        Double l_extendedPrice = lineItem.L_EXTENDEDPRICE;
        Double l_discount = lineItem.L_DISCOUNT;
        Double volumn = l_extendedPrice * (1 - l_discount);
        Date l_shipdate = lineItem.L_SHIPDATE;
        Calendar calendar = Calendar.getInstance();
        calendar.setTime(l_shipdate);

        int l_shipYear = calendar.get(Calendar.YEAR);
        Supplier supplier = (Supplier) supplier_Table.indexLiveTuple.get(l_suppkey);
        Long s_nationkey = supplier.S_NATIONKEY;
        Nation nation1 = (Nation) nation1_Table.indexLiveTuple.get(s_nationkey);
        String n_name1 = nation1.N_NAME;
        Orders order = (Orders) orders_Table.indexLiveTuple.get(l_orderkey);
        long o_custkey = order.O_CUSTKEY;
        Customer customer = (Customer) customer_Table.indexLiveTuple.get(o_custkey);
        Long c_nationkey = customer.C_NATIONKEY;
        Nation nation2 = (Nation) nation2_Table.indexLiveTuple.get(c_nationkey);
        String n_name2 = nation2.N_NAME;
        if (n_name1.equals(n_name2)) {
            return null;
        } else {
            return Tuple4.of(n_name1, n_name2, l_shipYear, volumn);
        }
    }

}
