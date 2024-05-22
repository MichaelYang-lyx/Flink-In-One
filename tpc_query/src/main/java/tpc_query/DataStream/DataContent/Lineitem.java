package tpc_query.DataStream.DataContent;

import java.sql.Date;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Set;

public class LineItem extends DataContent {
    public Long L_ORDERKEY;
    public int L_PARTKEY;
    public Long L_SUPPKEY;
    public Long L_LINENUMBER;
    public double L_QUANTITY;
    public double L_EXTENDEDPRICE;
    public double L_DISCOUNT;
    public double L_TAX;
    public char L_RETURNFLAG;
    public char L_LINESTATUS;
    public Date L_SHIPDATE;
    public String L_COMMITDATE;
    public String L_RECEIPTDATE;
    public String L_SHIPINSTRUCT;
    public String L_SHIPMODE;
    public String L_COMMENT;

    public LineItem() {
    }

    public LineItem(String[] string) {
        super();
        this.L_ORDERKEY = Long.parseLong(string[0]);
        this.L_PARTKEY = Integer.parseInt(string[1]);
        this.L_SUPPKEY = Long.parseLong(string[2]);
        this.L_LINENUMBER = Long.parseLong(string[3]);
        this.L_QUANTITY = Double.parseDouble(string[4]);
        this.L_EXTENDEDPRICE = Double.parseDouble(string[5]);
        this.L_DISCOUNT = Double.parseDouble(string[6]);
        this.L_TAX = Double.parseDouble(string[7]);
        this.L_RETURNFLAG = string[8].charAt(0);
        this.L_LINESTATUS = string[9].charAt(0);
        this.L_SHIPDATE = Date.valueOf(string[10]);
        this.L_COMMITDATE = string[11];
        this.L_RECEIPTDATE = string[12];
        this.L_SHIPINSTRUCT = string[13];
        this.L_SHIPMODE = string[14];
        this.L_COMMENT = string[15];
        this.foreignKeyMapping = this.getForeignKeyQ7();
    }

    public Long primaryKeyLong() {
        Long a = L_ORDERKEY;
        Long b = L_LINENUMBER;
        return a >= b ? a * a + a + b : a + b * b;
    }

    public String primaryKeySQL() {
        return "l_orderkey = " + String.valueOf(L_ORDERKEY) + " AND l_linenumber = " + String.valueOf(L_LINENUMBER);
    }

    public List<String> toList() {
        return Arrays.asList(String.valueOf(L_ORDERKEY), String.valueOf(L_PARTKEY), String.valueOf(L_SUPPKEY),
                String.valueOf(L_LINENUMBER), String.valueOf(L_QUANTITY), String.valueOf(L_EXTENDEDPRICE),
                String.valueOf(L_DISCOUNT), String.valueOf(L_TAX), String.valueOf(L_RETURNFLAG),
                String.valueOf(L_LINESTATUS), String.valueOf(L_SHIPDATE), L_COMMITDATE, L_RECEIPTDATE, L_SHIPINSTRUCT,
                L_SHIPMODE,
                L_COMMENT);
    }

    public String toString() {
        return "Lineitem [L_ORDERKEY=" + L_ORDERKEY + ", L_PARTKEY=" + L_PARTKEY + ", L_SUPPKEY=" + L_SUPPKEY
                + ", L_LINENUMBER=" + L_LINENUMBER + ", L_QUANTITY=" + L_QUANTITY + ", L_EXTENDEDPRICE="
                + L_EXTENDEDPRICE + ", L_DISCOUNT=" + L_DISCOUNT + ", L_TAX=" + L_TAX + ", L_RETURNFLAG=" + L_RETURNFLAG
                + ", L_LINESTATUS=" + L_LINESTATUS + ", L_SHIPDATE=" + L_SHIPDATE + ", L_COMMITDATE=" + L_COMMITDATE
                + ", L_RECEIPTDATE=" + L_RECEIPTDATE + ", L_SHIPINSTRUCT=" + L_SHIPINSTRUCT + ", L_SHIPMODE="
                + L_SHIPMODE + ", L_COMMENT=" + L_COMMENT + "]";
    }

    public HashMap<String, Long> getForeignKeyQ7() {

        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("SUPPLIER", L_SUPPKEY);
        foreignKeyMapping.put("ORDERS", L_ORDERKEY);

        return foreignKeyMapping;
    };

    public HashMap<String, Long> getForeignKeyQ5() {

        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("SUPPLIER", L_SUPPKEY);
        foreignKeyMapping.put("ORDERS", L_ORDERKEY);

        return foreignKeyMapping;
    };
}