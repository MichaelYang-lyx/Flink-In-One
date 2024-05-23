package tpc_query.Update;

import java.util.ArrayList;
import java.util.Arrays;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.Database.ITable;
import tpc_query.Database.MyTable;

import tpc_query.Query.Q7;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple4;

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
        // insert_instance.insert(null, null, null, null);

    }

    /**
     * Insert a tuple into the tables.
     * 
     * @param tables          The map of tables.
     * @param tableName       The name of the table.
     * @param dataContent     The data content of the tuple.
     * @param joinResultState The map state for join results.
     * @throws Exception If an error occurs during insertion.
     */
    public static void insert(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {
        MyTable thisTable = (MyTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();
        // Insert the new tuple into the table
        thisTable.allTuples.put(thisPrimaryKey, dataContent);

        // If the table is not a leaf node
        if (!thisTable.isLeaf) {
            thisTable.sCounter.put(thisPrimaryKey, 0);

            // Iterate over child tables
            for (String childName : thisTable.children) {

                thisTable.indexTableAndTableChildInfo.computeIfAbsent(childName,
                        k -> new HashMap<Long, ArrayList<Long>>());
                HashMap<Long, ArrayList<Long>> childRelation = thisTable.indexTableAndTableChildInfo.get(childName);
                Long tupleForeignKey = dataContent.getforeignKeyMapping().get(childName);
                ArrayList<Long> lst = childRelation.get(tupleForeignKey);

                // Ensure there is no duplicate primary key
                if (lst != null) {
                    if (lst.contains(thisPrimaryKey)) {
                        try {
                            throw new Exception(
                                    "Should not have the same primary key. Assign Lineitem Unique primary Key");
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                    lst.add(thisPrimaryKey);
                } else {
                    childRelation.put(tupleForeignKey, new ArrayList<>(Arrays.asList(thisPrimaryKey)));
                }

                MyTable childTable = (MyTable) tables.get(childName);
                // If πPK(Rc)t ∈ I(Rc) then s(t) ← s(t) + 1
                if (childTable.indexLiveTuple.containsKey(tupleForeignKey)) {
                    int curCount = thisTable.sCounter.get(thisPrimaryKey);
                    thisTable.sCounter.put(thisPrimaryKey, curCount + 1);
                }
            }
        }

        // If R is a leaf or s(t) = |C(R)| then insertUpdate
        if (thisTable.isLeaf || (thisTable.sCounter.get(thisPrimaryKey) == Math.abs(thisTable.numChild))) {
            insertUpdate(tables, tableName, dataContent, joinResultState);
        } else {
            // Otherwise, add to non-live tuples
            thisTable.indexNonLiveTuple.put(thisPrimaryKey, dataContent);
        }
    };

    /**
     * Perform insert update operation on the tables.
     * 
     * @param tables          The map of tables.
     * @param tableName       The name of the table.
     * @param dataContent     The data content of the tuple.
     * @param joinResultState The map state for join results.
     * @throws Exception If an error occurs during insert update.
     */
    public static void insertUpdate(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {

        MyTable thisTable = (MyTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();

        // Mark the tuple as live by adding it to the live tuple index
        thisTable.indexLiveTuple.put(thisPrimaryKey, dataContent);

        // If this table is the root and s(t) = |C(R)|, compute the query result
        if (thisTable.isRoot && (thisTable.sCounter.get(thisPrimaryKey) == thisTable.numChild)) {
            Tuple4<String, String, Integer, Double> result = Q7.selectResult(tables, thisPrimaryKey);
            if (result != null) {
                joinResultState.put(thisPrimaryKey, result); // Store the result if it's not null
            }
        } else {
            // Iterate over parent tables
            for (String parentName : thisTable.parents) {
                MyTable parentTable = (MyTable) tables.get(parentName);
                HashMap<Long, ArrayList<Long>> parentRelation = parentTable.indexTableAndTableChildInfo.get(tableName);

                if (parentRelation != null) {
                    ArrayList<Long> parentPKey = parentRelation.get(thisPrimaryKey);
                    if (parentPKey != null) {
                        for (Long tp : parentPKey) {
                            int curCount = parentTable.sCounter.get(tp);
                            parentTable.sCounter.put(tp, curCount + 1);

                            // If s(tp) = |C(Rp)|, mark the parent tuple as live
                            if (parentTable.sCounter.get(tp) == Math.abs(parentTable.numChild)) {
                                if (parentTable.allTuples.containsKey(tp)) {
                                    IDataContent parentContent = parentTable.allTuples.get(tp);
                                    parentTable.indexNonLiveTuple.remove(tp);

                                    // Recursively update the parent tuple
                                    insertUpdate(tables, parentTable.tableName, parentContent, joinResultState);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}
