package tpc_query.DataStream.DataContent;

public class Supplier implements IDataContent {
    private int S_SUPPKEY;
    private String S_NAME;
    private String S_ADDRESS;
    private int S_NATIONKEY;
    private String S_PHONE;
    private double S_ACCTBAL;
    private String S_COMMENT;

    public Supplier() {
    }

    public Supplier(String[] string) {
        this.S_SUPPKEY = Integer.parseInt(string[0]);
        this.S_NAME = string[1];
        this.S_ADDRESS = string[2];
        this.S_NATIONKEY = Integer.parseInt(string[3]);
        this.S_PHONE = string[4];
        this.S_ACCTBAL = Double.parseDouble(string[5]);
        this.S_COMMENT = string[6];
    }

    // getters and setters...

    public String toString() {
        return "Supplier { S_SUPPKEY=" + S_SUPPKEY + ", S_NAME=" + S_NAME + ", S_ADDRESS=" + S_ADDRESS
                + ", S_NATIONKEY="
                + S_NATIONKEY + ", S_PHONE=" + S_PHONE + ", S_ACCTBAL=" + S_ACCTBAL + ", S_COMMENT=" + S_COMMENT + "}";
    }
}