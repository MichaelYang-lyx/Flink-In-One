package tpc_query.DataStream.DataContent;

import java.util.HashMap;
import java.util.List;

public interface IDataContent {

    public HashMap<String, Long> getforeignKeyMapping();

    public String toString();

    public String primaryKeyString();

    public Long primaryKeyLong();

    public List<String> toList();
}
