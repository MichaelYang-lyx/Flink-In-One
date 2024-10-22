package tpc_query.DataStream.DataContent;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.flink.api.common.state.MapState;

public abstract class DataContent implements IDataContent {

    @Override
    public abstract String toString();

    @Override
    public abstract String primaryKeySQL();

    public abstract HashMap<String, Long> getForeignKeyQ7();

    public abstract HashMap<String, Long> getForeignKeyQ5();

    public HashMap<String, Long> foreignKeyMapping;

    /*
     * public Long primaryKeyLong() {
     * String str = this.primaryKeyString();
     * UUID uuid = UUID.nameUUIDFromBytes(str.getBytes());
     * Long longUuid = uuid.getMostSignificantBits();
     * return longUuid;
     * };
     */

    public HashMap<String, Long> getforeignKeyMapping() {

        return this.foreignKeyMapping;
    };

    @Override
    public abstract List<String> toList();
}
