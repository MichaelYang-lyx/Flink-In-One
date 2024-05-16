package tpc_query.DataStream.DataContent;

import java.util.List;

public interface IDataContent {

    public String toString();

    public String primaryKeyString();

    public Long primaryKeyLong();

    public List<String> toList();
}
