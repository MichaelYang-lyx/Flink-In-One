package tpc_query.Update;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;
import java.util.UUID;

import tpc_query.DataStream.DataOperation;
import tpc_query.Database.ITable;
import tpc_query.Database.MemoryTable;
import tpc_query.Database.MySQLTable;
import tpc_query.Database.Table;
import tpc_query.Database.TableController;
import tpc_query.Query.IQuery;
import tpc_query.Query.Q5;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.util.Collector;

public class Insert extends Update {
    public void run() {
        System.out.println("Insert");
    }

    public static void main(String[] args) {
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
        insert_instance.insert(null, null);

    }

    public void insert(MySQLTable table, DataOperation dataOperation) {

        // 这里
        if (!table.isLeaf) {
            table.sCounter.put(dataOperation.dataContent.primaryKeyLong(), 0);
            for (String childName : table.children) {
                // I(R, Rc ) ← I(R, Rc ) + (πPK(Rc )t → πPK(R),PK(Rc )t)
                // initialize I(R,Rc) if null
                System.out.println(childName);
                table.indexTableAndTableChildInfo.computeIfAbsent(childName, k -> new HashMap<Long, ArrayList<Long>>());
                Long tupleForeignKey = updateTuple.foreignKeyMapping.get(childName);
                // tupleTable.indexTableAndTableChildInfo.computeIfAbsent(childName,
                // k -> new HashMap<Long, ArrayList<Long>>());
            }

        }
    };
    /*
     * public void insertAlgorithm(MapState<String, TableLogger> allTableState,
     * Update updateTuple,
     * Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws
     * Exception {
     * if (updateTuple.operation.compareTo("+") == 0) {
     * // System.out.println("process table is " + updateTuple.tableName +
     * " insert ");
     * TableLogger tupleTable = allTableState.get(updateTuple.tableName);
     * tupleTable.allTuples.put(updateTuple.primaryKey, updateTuple);
     * // if R is not a leaf then
     * if (!tupleTable.isLeaf) {
     * // s <- 0 initialize this tuple counter, use its primary key as index
     * Hashtable<Long, Integer> sCounter = tupleTable.getsCounter();
     * sCounter.put(updateTuple.primaryKey, 0);
     * // for each Rc ∈ C(R) for each child table
     * for (String childName : tupleTable.childInfo) {
     * // I(R, Rc ) ← I(R, Rc ) + (πPK(Rc )t → πPK(R),PK(Rc )t)
     * // initialize I(R,Rc) if null
     * tupleTable.indexTableAndTableChildInfo.computeIfAbsent(childName,
     * k -> new HashMap<Long, ArrayList<Long>>());
     * Long tupleForeignKey = updateTuple.foreignKeyMapping.get(childName);
     * // initialize an arraylist if not exist the key
     * ArrayList<Long> lst =
     * tupleTable.indexTableAndTableChildInfo.get(childName).get(tupleForeignKey);
     * if (lst != null) {
     * if (lst.contains(updateTuple.primaryKey)) {
     * throw new
     * Exception("Should not have same primary key. Assign Lineitem Unique primary Key"
     * );
     * }
     * lst.add(updateTuple.primaryKey);
     * } else {
     * tupleTable.indexTableAndTableChildInfo.get(childName).put(tupleForeignKey,
     * new ArrayList<>(List.of(updateTuple.primaryKey)));
     * }
     * // if πPK(Rc)t ∈ I(Rc) then s(t) ← s(t) + 1
     * // if this tuple foreign key appear in child table live tuple set, increase
     * // tuple counter by 1
     * if (allTableState.get(childName).indexLiveTuple.containsKey(tupleForeignKey))
     * {
     * int curCount = sCounter.get(updateTuple.primaryKey);
     * sCounter.put(updateTuple.primaryKey, curCount + 1);
     * }
     * }
     * // update assertion key, q7 has no assertion key
     * }
     * // if R is a leaf or s(t) = |C(R)| then
     * if (tupleTable.isLeaf
     * || (tupleTable.getsCounter().get(updateTuple.primaryKey) ==
     * Math.abs(tupleTable.numChild))) {
     * // insert-Update(t, R,t) Algo
     * insertUpdateAlgorithm(tableState, updateTuple, collector);
     * }
     * 
     * // else I(N(R)) ← I(N(R)) + (πPK(R)t → t) put this tuple to non live tuple
     * set.
     * else {
     * tupleTable.indexNonLiveTuple.put(updateTuple.primaryKey, updateTuple);
     * }
     * }
     * }
     */

    public void insertUpdate(Table table, DataOperation dataOperation) {

        return;
    };

}
