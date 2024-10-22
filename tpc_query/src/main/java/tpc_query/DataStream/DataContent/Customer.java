package tpc_query.DataStream.DataContent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Customer extends DataContent {
    public Long C_CUSTKEY;
    public String C_NAME;
    public String C_ADDRESS;
    public Long C_NATIONKEY;
    public String C_PHONE;
    public double C_ACCTBAL;
    public String C_MKTSEGMENT;
    public String C_COMMENT;

    public Customer() {
    }

    public Customer(String[] string) {
        super();
        this.C_CUSTKEY = Long.parseLong(string[0]);
        this.C_NAME = string[1];
        this.C_ADDRESS = string[2];
        this.C_NATIONKEY = Long.parseLong(string[3]);
        this.C_PHONE = string[4];
        this.C_ACCTBAL = Double.parseDouble(string[5]);
        this.C_MKTSEGMENT = string[6];
        this.C_COMMENT = string[7];
        this.foreignKeyMapping = this.getForeignKeyQ7();

    }

    public Long primaryKeyLong() {
        return C_CUSTKEY;
    }

    public String primaryKeySQL() {
        return "c_custkey = " + String.valueOf(C_CUSTKEY);
    }

    public Long getC_CUSTKEY() {
        return C_CUSTKEY;
    }

    public String getC_NAME() {
        return C_NAME;
    }

    public String getC_ADDRESS() {
        return C_ADDRESS;
    }

    public Long getC_NATIONKEY() {
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

    public List<String> toList() {
        return Arrays.asList(String.valueOf(C_CUSTKEY), C_NAME, C_ADDRESS, String.valueOf(C_NATIONKEY), C_PHONE,
                String.valueOf(C_ACCTBAL), C_MKTSEGMENT, C_COMMENT);
    }

    public String toString() {
        return "Customer { C_CUSTKEY=" + C_CUSTKEY + ", C_NAME=" + C_NAME + ", C_ADDRESS=" + C_ADDRESS
                + ", C_NATIONKEY="
                + C_NATIONKEY + ", C_PHONE=" + C_PHONE + ", C_ACCTBAL=" + C_ACCTBAL + ", C_MKTSEGMENT=" + C_MKTSEGMENT
                + ", C_COMMENT=" + C_COMMENT + "}";
    }

    public HashMap<String, Long> getForeignKeyQ5() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("SUPPLIER", C_NATIONKEY);
        return foreignKeyMapping;
    }

    public HashMap<String, Long> getForeignKeyQ7() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("NATION2", C_NATIONKEY);
        return foreignKeyMapping;
    }
}
