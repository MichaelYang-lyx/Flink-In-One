package tpc_query.DataStream.DataContent;

import java.util.List;
import java.util.UUID;

public abstract class DataContent implements IDataContent {

    @Override
    public abstract String toString();

    @Override
    public abstract String primaryKeyString();

    public Long primaryKeyLong() {
        String str = this.primaryKeyString();
        UUID uuid = UUID.nameUUIDFromBytes(str.getBytes());
        Long longUuid = uuid.getMostSignificantBits();
        return longUuid;
    };

    @Override
    public abstract List<String> toList();
}
