package tpc_query.DataStream.DataContent;

public class Lineitem implements IDataContent {
    private int L_ORDERKEY;
    private int L_PARTKEY;
    private int L_SUPPKEY;
    private int L_LINENUMBER;
    private double L_QUANTITY;
    private double L_EXTENDEDPRICE;
    private double L_DISCOUNT;
    private double L_TAX;
    private char L_RETURNFLAG;
    private char L_LINESTATUS;
    private String L_SHIPDATE;
    private String L_COMMITDATE;
    private String L_RECEIPTDATE;
    private String L_SHIPINSTRUCT;
    private String L_SHIPMODE;
    private String L_COMMENT;

    public Lineitem() {
    }

    public Lineitem(String[] string) {
        this.L_ORDERKEY = Integer.parseInt(string[0]);
        this.L_PARTKEY = Integer.parseInt(string[1]);
        this.L_SUPPKEY = Integer.parseInt(string[2]);
        this.L_LINENUMBER = Integer.parseInt(string[3]);
        this.L_QUANTITY = Double.parseDouble(string[4]);
        this.L_EXTENDEDPRICE = Double.parseDouble(string[5]);
        this.L_DISCOUNT = Double.parseDouble(string[6]);
        this.L_TAX = Double.parseDouble(string[7]);
        this.L_RETURNFLAG = string[8].charAt(0);
        this.L_LINESTATUS = string[9].charAt(0);
        this.L_SHIPDATE = string[10];
        this.L_COMMITDATE = string[11];
        this.L_RECEIPTDATE = string[12];
        this.L_SHIPINSTRUCT = string[13];
        this.L_SHIPMODE = string[14];
        this.L_COMMENT = string[15];
    }

    // getters and setters...

    public String toString() {
        return "Lineitem [L_ORDERKEY=" + L_ORDERKEY + ", L_PARTKEY=" + L_PARTKEY + ", L_SUPPKEY=" + L_SUPPKEY
                + ", L_LINENUMBER=" + L_LINENUMBER + ", L_QUANTITY=" + L_QUANTITY + ", L_EXTENDEDPRICE="
                + L_EXTENDEDPRICE + ", L_DISCOUNT=" + L_DISCOUNT + ", L_TAX=" + L_TAX + ", L_RETURNFLAG=" + L_RETURNFLAG
                + ", L_LINESTATUS=" + L_LINESTATUS + ", L_SHIPDATE=" + L_SHIPDATE + ", L_COMMITDATE=" + L_COMMITDATE
                + ", L_RECEIPTDATE=" + L_RECEIPTDATE + ", L_SHIPINSTRUCT=" + L_SHIPINSTRUCT + ", L_SHIPMODE="
                + L_SHIPMODE + ", L_COMMENT=" + L_COMMENT + "]";
    }
}