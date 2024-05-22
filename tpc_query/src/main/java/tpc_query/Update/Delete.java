package tpc_query.Update;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.java.tuple.Tuple4;

import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.Database.ITable;
import tpc_query.Database.MySQLTable;
import tpc_query.Query.Q7;

public class Delete extends Update {
    public void run() {
        System.out.println("Delete");
    }

    public static void delete(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {

        MySQLTable thisTable = (MySQLTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();
        thisTable.allTuples.remove(dataContent.primaryKeyLong());

        // if t ∈ L(R) then if it is in live tuple set
        if (thisTable.indexLiveTuple.containsKey(thisPrimaryKey)) {
            deleteUpdate(tables, tableName, dataContent, joinResultState);
        } else {

            thisTable.indexNonLiveTuple.remove(dataContent.primaryKeyLong());
        }

        // if table is not the root
        if (!thisTable.isRoot) {
            // I(Rp, R) ← I(Rp, R) − (πPK(R)t → πPK(Rp ),PK(R)t)
            for (String parentName : thisTable.parents) {
                MySQLTable parentTable = (MySQLTable) tables.get(parentName);
                HashMap<Long, ArrayList<Long>> parentRelation = parentTable.indexTableAndTableChildInfo.get(tableName);
                if (parentRelation != null) {
                    parentRelation.remove(thisPrimaryKey);
                }

            }

        }

    };

    public static void deleteUpdate(Map<String, ITable> tables, String tableName, IDataContent dataContent,
            MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState) throws Exception {

        MySQLTable thisTable = (MySQLTable) tables.get(tableName);
        Long thisPrimaryKey = dataContent.primaryKeyLong();
        thisTable.indexLiveTuple.remove(thisPrimaryKey);

        if (thisTable.isRoot) {
            joinResultState.remove(thisPrimaryKey);
        } else {
            for (String parentName : thisTable.parents) {
                MySQLTable parentTable = (MySQLTable) tables.get(parentName);
                HashMap<Long, ArrayList<Long>> parentRelation = parentTable.indexTableAndTableChildInfo.get(tableName);
                if (parentRelation != null) {
                    ArrayList<Long> parentPKey = parentRelation.get(thisPrimaryKey);

                    if (parentPKey != null) {
                        for (Long tp : parentPKey) {
                            // if tp ∈ N(Rp ) then
                            if (parentTable.indexNonLiveTuple.containsKey(tp)) {
                                // s(tp ) ← s(tp ) - 1
                                int curCount = parentTable.sCounter.get(tp);
                                parentTable.sCounter.put(tp, curCount - 1);

                            } else {
                                // s(tp ) ← |C(R)| − 1
                                parentTable.sCounter.put(tp, Math.abs(thisTable.numChild) - 1);

                                if (parentTable.allTuples.containsKey(tp)) {
                                    IDataContent parentContent = parentTable.allTuples.get(tp);
                                    parentTable.indexNonLiveTuple.put(tp, parentContent);
                                    // Delete-Update(tp, Rp, join_result tp )
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
