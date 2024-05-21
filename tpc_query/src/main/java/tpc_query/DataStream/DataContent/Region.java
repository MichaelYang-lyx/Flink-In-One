package tpc_query.DataStream.DataContent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Region extends DataContent {
    private Long R_REGIONKEY;
    private String R_NAME;
    private String R_COMMENT;

    public Region() {
    }

    public Region(String[] string) {
        super();
        this.R_REGIONKEY = Long.parseLong(string[0]);
        this.R_NAME = string[1];
        this.R_COMMENT = string[2];
        this.foreignKeyMapping = this.getForeignKeyQ7();
    }

    public String primaryKeyString() {
        return String.valueOf(R_REGIONKEY);
    }

    public Long primaryKeyLong() {
        return R_REGIONKEY;
    }

    public Long getR_REGIONKEY() {
        return R_REGIONKEY;
    }

    public String getR_NAME() {
        return R_NAME;
    }

    public String getR_COMMENT() {
        return R_COMMENT;
    }

    public List<String> toList() {
        return Arrays.asList(String.valueOf(R_REGIONKEY), R_NAME, R_COMMENT);
    }

    public String toString() {
        return "Region [R_REGIONKEY=" + R_REGIONKEY + ", R_NAME=" + R_NAME + ", R_COMMENT=" + R_COMMENT + "]";
    }

    public HashMap<String, Long> getForeignKeyQ7() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        return foreignKeyMapping;
    };

    public HashMap<String, Long> getForeignKeyQ5() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        return foreignKeyMapping;
    };
}
