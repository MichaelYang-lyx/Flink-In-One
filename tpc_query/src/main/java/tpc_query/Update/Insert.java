package tpc_query.Update;

import java.util.ArrayList;
import java.util.Hashtable;
import tpc_query.Database.ITable;
import tpc_query.Database.Table;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.util.Collector;

public class Insert extends Update {
    public void run() {
        System.out.println("Insert");
    }

    public void insert(Table table) {
        if (!table.isLeaf) {

        }
    };

    public void insertAlgorithm(MapState<String, TableLogger> allTableState, Update updateTuple,
            Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        if (updateTuple.operation.compareTo("+") == 0) {
            // System.out.println("process table is " + updateTuple.tableName + " insert ");
            TableLogger tupleTable = allTableState.get(updateTuple.tableName);
            tupleTable.allTuples.put(updateTuple.primaryKey, updateTuple);
            // if R is not a leaf then
            if (!tupleTable.isLeaf) {
                // s <- 0 initialize this tuple counter, use its primary key as index
                Hashtable<Long, Integer> sCounter = tupleTable.getsCounter();
                sCounter.put(updateTuple.primaryKey, 0);
                // for each Rc ∈ C(R) for each child table
                for (String childName : tupleTable.childInfo) {
                    // I(R, Rc ) ← I(R, Rc ) + (πPK(Rc )t → πPK(R),PK(Rc )t)
                    // initialize I(R,Rc) if null
                    tupleTable.indexTableAndTableChildInfo.computeIfAbsent(childName,
                            k -> new HashMap<Long, ArrayList<Long>>());
                    Long tupleForeignKey = updateTuple.foreignKeyMapping.get(childName);
                    // initialize an arraylist if not exist the key
                    ArrayList<Long> lst = tupleTable.indexTableAndTableChildInfo.get(childName).get(tupleForeignKey);
                    if (lst != null) {
                        if (lst.contains(updateTuple.primaryKey)) {
                            throw new Exception("Should not have same primary key. Assign Lineitem Unique primary Key");
                        }
                        lst.add(updateTuple.primaryKey);
                    } else {
                        tupleTable.indexTableAndTableChildInfo.get(childName).put(tupleForeignKey,
                                new ArrayList<>(List.of(updateTuple.primaryKey)));
                    }
                    // if πPK(Rc)t ∈ I(Rc) then s(t) ← s(t) + 1
                    // if this tuple foreign key appear in child table live tuple set, increase
                    // tuple counter by 1
                    if (allTableState.get(childName).indexLiveTuple.containsKey(tupleForeignKey)) {
                        int curCount = sCounter.get(updateTuple.primaryKey);
                        sCounter.put(updateTuple.primaryKey, curCount + 1);
                    }
                }
                // update assertion key, q7 has no assertion key
            }
            // if R is a leaf or s(t) = |C(R)| then
            if (tupleTable.isLeaf
                    || (tupleTable.getsCounter().get(updateTuple.primaryKey) == Math.abs(tupleTable.numChild))) {
                // insert-Update(t, R,t) Algo
                insertUpdateAlgorithm(tableState, updateTuple, collector);
            }

            // else I(N(R)) ← I(N(R)) + (πPK(R)t → t) put this tuple to non live tuple set.
            else {
                tupleTable.indexNonLiveTuple.put(updateTuple.primaryKey, updateTuple);
            }
        }
    }

    public void insertUpdateAlgorithm(MapState<String, TableLogger> allTableState, Update updateTuple,
            Collector<List<Tuple4<String, String, Integer, Double>>> collector) throws Exception {
        // I(L(R)) ← I(L(R)) + (πPK(R)t → t) // put this tuple to live tuple set
        // System.out.println("process insert update");
        TableLogger tupleTable = allTableState.get(updateTuple.tableName);
        tupleTable.indexLiveTuple.put(updateTuple.primaryKey, updateTuple);
        // if R is the root of T then ∆Q ← ∆Q ∪ {join_result }
        if (tupleTable.isRoot && (tupleTable.sCounter.get(updateTuple.primaryKey) == tupleTable.numChild)) {
            // Perform Join
            // System.out.println("Insert Tuple " + updateTuple.toString());
            Tuple4<String, String, Integer, Double> result = getSelectedTuple(allTableState, updateTuple.primaryKey);
            joinResultState.put(updateTuple.primaryKey, result);

        }
        // else P ← look up I(Rp, R) with key πPK(R)t
        else {
            TableLogger parentTable = allTableState.get(tupleTable.parentName);
            HashMap<Long, ArrayList<Long>> IRpAndR = parentTable.indexTableAndTableChildInfo.get(tupleTable.tableName);
            if (IRpAndR != null) {
                ArrayList<Long> parentPKey = IRpAndR.get(updateTuple.primaryKey);
                // for each tp ∈ P do
                if (parentPKey != null) {
                    for (Long tp : parentPKey) {

                        // s(tp ) ← s(tp ) + 1
                        int curCount = parentTable.sCounter.get(tp);
                        parentTable.sCounter.put(tp, curCount + 1);
                        // if s(tp ) = |C(Rp )| then

                        if (parentTable.sCounter.get(tp) == Math.abs(parentTable.numChild)) {
                            // I(N(Rp )) ← I(N(Rp )) − (πPK(Rp ) tp → tp ) update non live tuple set
                            if (parentTable.allTuples.containsKey(tp)) {
                                Update parentTuple = parentTable.allTuples.get(tp);
                                parentTable.indexNonLiveTuple.remove(tp);
                                // Insert-Update(tp, Rp, join_result tp )
                                insertUpdateAlgorithm(tableState, parentTuple, collector);
                            }
                        }
                    }
                }
            }
        }
    }
}
