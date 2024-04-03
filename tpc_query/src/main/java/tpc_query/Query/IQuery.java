package tpc_query.Query;

import tpc_query.DataStream.DataOperation;

public interface IQuery {
    default boolean filter(DataOperation dataOperation) {
        return true;
    }
}