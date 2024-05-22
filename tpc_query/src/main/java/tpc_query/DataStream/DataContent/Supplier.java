package tpc_query.DataStream.DataContent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Supplier extends DataContent {
    public Long S_SUPPKEY;
    public String S_NAME;
    public String S_ADDRESS;
    public Long S_NATIONKEY;
    public String S_PHONE;
    public double S_ACCTBAL;
    public String S_COMMENT;

    public Supplier() {
    }

    public Supplier(String[] string) {
        this.S_SUPPKEY = Long.parseLong(string[0]);
        this.S_NAME = string[1];
        this.S_ADDRESS = string[2];
        this.S_NATIONKEY = Long.parseLong(string[3]);
        this.S_PHONE = string[4];
        this.S_ACCTBAL = Double.parseDouble(string[5]);
        this.S_COMMENT = string[6];
        this.foreignKeyMapping = this.getForeignKeyQ7();
    }

    public Long primaryKeyLong() {
        return S_SUPPKEY;
    }

    public List<String> toList() {
        return Arrays.asList(
                String.valueOf(S_SUPPKEY),
                S_NAME,
                S_ADDRESS,
                String.valueOf(S_NATIONKEY),
                S_PHONE,
                String.valueOf(S_ACCTBAL),
                S_COMMENT);
    }

    public String primaryKeySQL() {
        return "s_suppkey = " + String.valueOf(S_SUPPKEY);
    }

    public String toString() {
        return "Supplier { S_SUPPKEY=" + S_SUPPKEY + ", S_NAME=" + S_NAME + ", S_ADDRESS=" + S_ADDRESS
                + ", S_NATIONKEY="
                + S_NATIONKEY + ", S_PHONE=" + S_PHONE + ", S_ACCTBAL=" + S_ACCTBAL + ", S_COMMENT=" + S_COMMENT + "}";
    }

    public HashMap<String, Long> getForeignKeyQ5() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("NATION", S_NATIONKEY);

        return foreignKeyMapping;
    }

    public HashMap<String, Long> getForeignKeyQ7() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("NATION1", S_NATIONKEY);

        return foreignKeyMapping;
    }
}