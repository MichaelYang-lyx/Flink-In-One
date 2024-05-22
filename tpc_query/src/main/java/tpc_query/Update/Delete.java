package tpc_query.Update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple4;

import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.Database.ITable;
import tpc_query.Database.MyTable;
import tpc_query.Query.Q7;

/**
 * The Delete class represents a delete operation in the TPC query system.
 * It extends the Update class.
 */
public class Delete extends Update {

    /**
     * Runs the delete operation.
     */
    public void run() {
        System.out.println("Delete");
    }

    /**
     * Deletes a tuple from a table.
     *
     * @param tables          The map of table names to table objects.
     * @param tableName       The name of the table to delete from.
     * @param dataContent     The data content of the tuple to delete.
     * @param joinResultState The state of the join result.
     * @throws Exception If an error occurs during the delete operation.
     */
    public static void delete(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {

        MyTable thisTable = (MyTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();

        // Remove the tuple from the table
        thisTable.allTuples.remove(thisPrimaryKey);

        // If the tuple is live, perform deleteUpdate
        if (thisTable.indexLiveTuple.containsKey(thisPrimaryKey)) {
            deleteUpdate(tables, tableName, dataContent, joinResultState);
        } else {
            // Otherwise, remove it from the non-live tuple index
            thisTable.indexNonLiveTuple.remove(thisPrimaryKey);
        }

        // If the table is not the root, update parent relations
        if (!thisTable.isRoot) {
            // I(Rp, R) ← I(Rp, R) − (πPK(R)t → πPK(Rp ),PK(R)t)
            for (String parentName : thisTable.parents) {
                MyTable parentTable = (MyTable) tables.get(parentName);
                HashMap<Long, ArrayList<Long>> parentRelation = parentTable.indexTableAndTableChildInfo.get(tableName);
                if (parentRelation != null) {
                    parentRelation.remove(thisPrimaryKey);
                }
            }
        }
    }

    /**
     * Performs the delete update operation.
     *
     * @param tables          The map of table names to table objects.
     * @param tableName       The name of the table to perform the delete update on.
     * @param dataContent     The data content of the tuple to perform the delete
     *                        update on.
     * @param joinResultState The state of the join result.
     * @throws Exception If an error occurs during the delete update operation.
     */
    public static void deleteUpdate(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {

        MyTable thisTable = (MyTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();
        // Remove the tuple from the live tuple index
        thisTable.indexLiveTuple.remove(thisPrimaryKey);

        // If this table is the root, remove the join result
        if (thisTable.isRoot) {
            joinResultState.remove(thisPrimaryKey);
        } else {
            // Iterate over parent tables
            for (String parentName : thisTable.parents) {
                MyTable parentTable = (MyTable) tables.get(parentName);
                HashMap<Long, ArrayList<Long>> parentRelation = parentTable.indexTableAndTableChildInfo.get(tableName);
                if (parentRelation != null) {
                    ArrayList<Long> parentPKey = parentRelation.get(thisPrimaryKey);

                    if (parentPKey != null) {
                        for (Long tp : parentPKey) {
                            // If the parent tuple is non-live, decrement the sCounter
                            if (parentTable.indexNonLiveTuple.containsKey(tp)) {
                                int curCount = parentTable.sCounter.get(tp);
                                parentTable.sCounter.put(tp, curCount - 1);
                            } else {
                                // Otherwise, set the sCounter to the number of child tables minus 1
                                parentTable.sCounter.put(tp, Math.abs(thisTable.numChild) - 1);

                                if (parentTable.allTuples.containsKey(tp)) {
                                    IDataContent parentContent = parentTable.allTuples.get(tp);
                                    parentTable.indexNonLiveTuple.put(tp, parentContent);

                                    // Recursively update the parent tuple
                                    deleteUpdate(tables, parentTable.tableName, parentContent, joinResultState);
                                }
                            }
                        }
                    }
                }
            }
        }
    }

}
