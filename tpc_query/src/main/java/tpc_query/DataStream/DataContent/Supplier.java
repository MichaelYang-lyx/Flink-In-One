package tpc_query.DataStream.DataContent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Supplier extends DataContent {
    private int S_SUPPKEY;
    private String S_NAME;
    private String S_ADDRESS;
    private Long S_NATIONKEY;
    private String S_PHONE;
    private double S_ACCTBAL;
    private String S_COMMENT;

    public Supplier() {
    }

    public Supplier(String[] string) {
        this.S_SUPPKEY = Integer.parseInt(string[0]);
        this.S_NAME = string[1];
        this.S_ADDRESS = string[2];
        this.S_NATIONKEY = Long.parseLong(string[3]);
        this.S_PHONE = string[4];
        this.S_ACCTBAL = Double.parseDouble(string[5]);
        this.S_COMMENT = string[6];
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

    public String primaryKeyString() {
        return String.valueOf(S_SUPPKEY);
    }

    public String toString() {
        return "Supplier { S_SUPPKEY=" + S_SUPPKEY + ", S_NAME=" + S_NAME + ", S_ADDRESS=" + S_ADDRESS
                + ", S_NATIONKEY="
                + S_NATIONKEY + ", S_PHONE=" + S_PHONE + ", S_ACCTBAL=" + S_ACCTBAL + ", S_COMMENT=" + S_COMMENT + "}";
    }

    public HashMap<String, Long> getForeignKey() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("Nation1", S_NATIONKEY);
        return foreignKeyMapping;
    }
}