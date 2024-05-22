package tpc_query.Update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.DataContent.Customer;
import tpc_query.DataStream.DataContent.DataContent;
import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.DataStream.DataContent.LineItem;
import tpc_query.DataStream.DataContent.Nation;
import tpc_query.DataStream.DataContent.Orders;
import tpc_query.DataStream.DataContent.Supplier;
import tpc_query.Database.ITable;
import tpc_query.Database.MemoryTable;
import tpc_query.Database.MySQLTable;
import tpc_query.Database.Table;
import tpc_query.Database.TableController;
import tpc_query.Query.IQuery;
import tpc_query.Query.Q5;
import tpc_query.Query.Q7;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import com.ibm.icu.util.Calendar;

public class Insert extends Update {
    public void run() {
        System.out.println("Insert");
    }

    public static void main(String[] args) throws Exception {
        /*
         * IQuery query = new Q5();
         * TableController tableController = new TableController("memory");
         * tableController.setupTables(query);
         * MemoryTable table = (MemoryTable) tableController.tables.get("Customer");
         */
        // insert(table, null);
        String str = "你的字符串";
        UUID uuid = UUID.nameUUIDFromBytes(str.getBytes());
        long longUuid = uuid.getMostSignificantBits();
        String strUuid = uuid.toString();
        System.out.println("uuid: " + uuid);
        System.out.println("longUuid: " + longUuid);
        System.out.println("strUuid: " + strUuid);
        Insert insert_instance = new Insert();
        insert_instance.insert(null, null, null, null);

    }

    public void insert(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {

        MySQLTable thisTable = (MySQLTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();
        thisTable.allTuples.put(thisPrimaryKey, dataContent);
        if (!thisTable.isLeaf) {
            thisTable.sCounter.put(thisPrimaryKey, 0);
            for (String childName : thisTable.children) {
                // I(R, Rc ) ← I(R, Rc ) + (πPK(Rc )t → πPK(R),PK(Rc )t)
                thisTable.indexTableAndTableChildInfo.computeIfAbsent(childName,
                        k -> new HashMap<Long, ArrayList<Long>>());
                HashMap<Long, ArrayList<Long>> childRelation = thisTable.indexTableAndTableChildInfo.get(childName);
                // 这个tuple在child table中的外键
                Long tupleForeignKey = dataContent.getforeignKeyMapping().get(childName);

                // initialize an arraylist if not exist the key
                // lst: 这个child 外键->这个tuble的主键list
                ArrayList<Long> lst = childRelation.get(tupleForeignKey);
                if (lst != null) {

                    if (lst.contains(thisPrimaryKey)) {
                        try {
                            throw new Exception("Should not have same primary key. Assign Lineitem Unique primary Key");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    lst.add(thisPrimaryKey);

                } else {
                    childRelation.put(tupleForeignKey, new ArrayList<>(Arrays.asList(thisPrimaryKey)));
                }

                MySQLTable childTable = (MySQLTable) tables.get(childName);
                // if πPK(Rc)t ∈ I(Rc) then s(t) ← s(t) + 1

                if (childTable.indexLiveTuple.containsKey(tupleForeignKey)) {
                    int curCount = thisTable.sCounter.get(thisPrimaryKey);
                    thisTable.sCounter.put(thisPrimaryKey, curCount + 1);
                }

                // 这后面好像得加点东西

            }

        }
        // if R is a leaf or s(t) = |C(R)| then

        if (thisTable.isLeaf || (thisTable.sCounter.get(thisPrimaryKey) == Math.abs(thisTable.numChild))) {
            insertUpdate(tables, tableName, dataContent, joinResultState);
            // insertUpdateAlgorithm(tableState, updateTuple, collector);
        }

        // else I(N(R)) ← I(N(R)) + (πPK(R)t → t) put this tuple to non live tuple set.
        else {
            thisTable.indexNonLiveTuple.put(thisPrimaryKey, dataContent);
        }
    };

    public void insertUpdate(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {

        MySQLTable thisTable = (MySQLTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();
        if (tableName.equals("LINEITEM")) {
            System.out.println("@@@@@@@@@");
        }
        thisTable.indexLiveTuple.put(thisPrimaryKey, dataContent);
        if (thisTable.isRoot && (thisTable.sCounter.get(thisPrimaryKey) == thisTable.numChild)) {
            Tuple4<String, String, Integer, Double> result = Q7.selectResult(tables, thisPrimaryKey);
            joinResultState.put(thisPrimaryKey, result);
            System.out.println("!!!!!!!!!!!!Select Result!!!!!!!!!!");
            System.out.println(result);

        } else {
            System.out.println("=============== Parents =================");
            System.out.println(thisTable);
            for (String parentName : thisTable.parents) {
                MySQLTable parentTable = (MySQLTable) tables.get(parentName);
                HashMap<Long, ArrayList<Long>> parentRelation = parentTable.indexTableAndTableChildInfo.get(tableName);

                if (parentRelation != null) {
                    ArrayList<Long> parentPKey = parentRelation.get(thisPrimaryKey);
                    if (parentPKey != null) {
                        for (Long tp : parentPKey) {
                            // s(tp ) ← s(tp ) + 1
                            int curCount = parentTable.sCounter.get(tp);
                            parentTable.sCounter.put(tp, curCount + 1);

                            // s(tp ) = |C(Rp )|
                            if (parentTable.sCounter.get(tp) == Math.abs(parentTable.numChild)) {

                                // 这里要加东西？
                                // I(N(Rp )) ← I(N(Rp )) − (πPK(Rp ) tp → tp ) update non live tuple set
                                if (parentTable.allTuples.containsKey(tp)) {
                                    IDataContent parentContent = parentTable.allTuples.get(tp);
                                    parentTable.indexNonLiveTuple.remove(tp);
                                    // Insert-Update(tp, Rp, join_result tp )
                                    insertUpdate(tables, parentTable.tableName, parentContent, joinResultState);
                                }

                            }
                        }
                    }
                }
            }
        }
    }

    public void insertUpdate(Table table, DataOperation dataOperation) {

        return;
    };

}
