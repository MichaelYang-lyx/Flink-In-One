package tpc_query.DataStream.DataContent;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;

public class Orders extends DataContent {
    public Long O_ORDERKEY;
    public Long O_CUSTKEY;
    public char O_ORDERSTATUS;
    public double O_TOTALPRICE;
    public String O_ORDERDATE;
    public String O_ORDERPRIORITY;
    public String O_CLERK;
    public int O_SHIPPRIORITY;
    public String O_COMMENT;

    public Orders() {
    }

    public Orders(String[] string) {
        super();
        this.O_ORDERKEY = Long.parseLong(string[0]);
        this.O_CUSTKEY = Long.parseLong(string[1]);
        this.O_ORDERSTATUS = string[2].charAt(0);
        this.O_TOTALPRICE = Double.parseDouble(string[3]);
        this.O_ORDERDATE = string[4];
        this.O_ORDERPRIORITY = string[5];
        this.O_CLERK = string[6];
        this.O_SHIPPRIORITY = Integer.parseInt(string[7]);
        this.O_COMMENT = string[8];
        this.foreignKeyMapping = this.getForeignKeyQ7();
    }

    public Long primaryKeyLong() {
        return O_ORDERKEY;
    }

    public String primaryKeySQL() {
        return "o_orderkey = " + String.valueOf(O_ORDERKEY);
    }

    public Long getO_ORDERKEY() {
        return O_ORDERKEY;
    }

    public Long getO_CUSTKEY() {
        return O_CUSTKEY;
    }

    public char getO_ORDERSTATUS() {
        return O_ORDERSTATUS;
    }

    public double getO_TOTALPRICE() {
        return O_TOTALPRICE;
    }

    public String getO_ORDERDATE() {
        return O_ORDERDATE;
    }

    public String getO_ORDERPRIORITY() {
        return O_ORDERPRIORITY;
    }

    public String getO_CLERK() {
        return O_CLERK;
    }

    public int getO_SHIPPRIORITY() {
        return O_SHIPPRIORITY;
    }

    public String getO_COMMENT() {
        return O_COMMENT;
    }

    public List<String> toList() {
        return Arrays.asList(String.valueOf(O_ORDERKEY), String.valueOf(O_CUSTKEY), String.valueOf(O_ORDERSTATUS),
                String.valueOf(O_TOTALPRICE), O_ORDERDATE, O_ORDERPRIORITY, O_CLERK, String.valueOf(O_SHIPPRIORITY),
                O_COMMENT);
    }

    public String toString() {
        return "Orders [O_ORDERKEY=" + O_ORDERKEY + ", O_CUSTKEY=" + O_CUSTKEY + ", O_ORDERSTATUS=" + O_ORDERSTATUS
                + ", O_TOTALPRICE=" + O_TOTALPRICE + ", O_ORDERDATE=" + O_ORDERDATE + ", O_ORDERPRIORITY="
                + O_ORDERPRIORITY + ", O_CLERK=" + O_CLERK + ", O_SHIPPRIORITY=" + O_SHIPPRIORITY + ", O_COMMENT="
                + O_COMMENT + "]";
    }

    public HashMap<String, Long> getForeignKeyQ() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("CUSTOMER", O_CUSTKEY);

        return foreignKeyMapping;
    };

    public HashMap<String, Long> getForeignKeyQ7() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("CUSTOMER", O_CUSTKEY);

        return foreignKeyMapping;
    };

    public HashMap<String, Long> getForeignKeyQ5() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("CUSTOMER", O_CUSTKEY);

        return foreignKeyMapping;
    };

}
