package tpc_query.DataStream.DataContent;

public class Customer implements IDataContent {
    private int C_CUSTKEY;
    private String C_NAME;
    private String C_ADDRESS;
    private int C_NATIONKEY;
    private String C_PHONE;
    private double C_ACCTBAL;
    private String C_MKTSEGMENT;
    private String C_COMMENT;

    public Customer() {
    }

    public Customer(String[] string) {
        this.C_CUSTKEY = Integer.parseInt(string[0]);
        this.C_NAME = string[1];
        this.C_ADDRESS = string[2];
        this.C_NATIONKEY = Integer.parseInt(string[3]);
        this.C_PHONE = string[4];
        this.C_ACCTBAL = Double.parseDouble(string[5]);
        this.C_MKTSEGMENT = string[6];
        this.C_COMMENT = string[7];
    }

    public int getC_CUSTKEY() {
        return C_CUSTKEY;
    }

    public String getC_NAME() {
        return C_NAME;
    }

    public String getC_ADDRESS() {
        return C_ADDRESS;
    }

    public int getC_NATIONKEY() {
        return C_NATIONKEY;
    }

    public String getC_PHONE() {
        return C_PHONE;
    }

    public double getC_ACCTBAL() {
        return C_ACCTBAL;
    }

    public String getC_MKTSEGMENT() {
        return C_MKTSEGMENT;
    }

    public String getC_COMMENT() {
        return C_COMMENT;
    }

    public String toString() {
        return "Customer { C_CUSTKEY=" + C_CUSTKEY + ", C_NAME=" + C_NAME + ", C_ADDRESS=" + C_ADDRESS
                + ", C_NATIONKEY="
                + C_NATIONKEY + ", C_PHONE=" + C_PHONE + ", C_ACCTBAL=" + C_ACCTBAL + ", C_MKTSEGMENT=" + C_MKTSEGMENT
                + ", C_COMMENT=" + C_COMMENT + "}";
    }
}
