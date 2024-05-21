package tpc_query.DataStream.DataContent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Nation extends DataContent {
    public Long N_NATIONKEY;
    public String N_NAME;
    public Long N_REGIONKEY;
    public String N_COMMENT;

    public Nation() {
    }

    public Nation(String[] string) {
        super();
        this.N_NATIONKEY = Long.parseLong(string[0]);
        this.N_NAME = string[1];
        this.N_REGIONKEY = Long.parseLong(string[2]);
        this.N_COMMENT = string[3];
        this.foreignKeyMapping = this.getForeignKeyQ7();
    }

    public String primaryKeyString() {
        return String.valueOf(N_NATIONKEY);
    }

    public Long primaryKeyLong() {
        return N_NATIONKEY;
    }

    public List<String> toList() {
        return Arrays.asList(String.valueOf(N_NATIONKEY), N_NAME, String.valueOf(N_REGIONKEY), N_COMMENT);
    }

    public String toString() {
        return "Nation [N_NATIONKEY=" + N_NATIONKEY + ", N_NAME=" + N_NAME + ", N_REGIONKEY=" + N_REGIONKEY
                + ", N_COMMENT=" + N_COMMENT + "]";
    }

    public HashMap<String, Long> getForeignKeyQ7() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        return foreignKeyMapping;
    };

    public HashMap<String, Long> getForeignKeyQ5() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("REGION", N_REGIONKEY);
        return foreignKeyMapping;
    };

}