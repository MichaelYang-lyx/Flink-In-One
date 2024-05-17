package tpc_query.DataStream.DataContent;

import java.util.HashMap;
import java.util.List;
import java.util.UUID;

public abstract class DataContent implements IDataContent {

    @Override
    public abstract String toString();

    @Override
    public abstract String primaryKeyString();

    public abstract HashMap<String, Long> getForeignKey();

    public HashMap<String, Long> foreignKeyMapping;

    public DataContent() {
        this.foreignKeyMapping = this.getForeignKey();
    }

    public Long primaryKeyLong() {
        String str = this.primaryKeyString();
        UUID uuid = UUID.nameUUIDFromBytes(str.getBytes());
        Long longUuid = uuid.getMostSignificantBits();
        return longUuid;
    };

    public HashMap<String, Long> getforeignKeyMapping() {
        return this.foreignKeyMapping;
    };

    @Override
    public abstract List<String> toList();
}
