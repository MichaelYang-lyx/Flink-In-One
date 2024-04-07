package tpc_query.Query;

import java.util.List;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple5;

import tpc_query.DataStream.DataOperation;

public interface IQuery {
    default boolean filter(DataOperation dataOperation) {
        return true;
    }

    public void registerTables(Map<String, Tuple5<Boolean, Boolean, String, Integer, List<String>>> tableMap);
}