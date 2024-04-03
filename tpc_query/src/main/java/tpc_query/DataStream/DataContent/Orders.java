package tpc_query.DataStream.DataContent;

public class Orders implements IDataContent {
    private int O_ORDERKEY;
    private int O_CUSTKEY;
    private char O_ORDERSTATUS;
    private double O_TOTALPRICE;
    private String O_ORDERDATE;
    private String O_ORDERPRIORITY;
    private String O_CLERK;
    private int O_SHIPPRIORITY;
    private String O_COMMENT;

    public Orders() {
    }

    public Orders(String[] string) {
        this.O_ORDERKEY = Integer.parseInt(string[0]);
        this.O_CUSTKEY = Integer.parseInt(string[1]);
        this.O_ORDERSTATUS = string[2].charAt(0);
        this.O_TOTALPRICE = Double.parseDouble(string[3]);
        this.O_ORDERDATE = string[4];
        this.O_ORDERPRIORITY = string[5];
        this.O_CLERK = string[6];
        this.O_SHIPPRIORITY = Integer.parseInt(string[7]);
        this.O_COMMENT = string[8];
    }

    // getters and setters...

    public String toString() {
        return "Orders [O_ORDERKEY=" + O_ORDERKEY + ", O_CUSTKEY=" + O_CUSTKEY + ", O_ORDERSTATUS=" + O_ORDERSTATUS
                + ", O_TOTALPRICE=" + O_TOTALPRICE + ", O_ORDERDATE=" + O_ORDERDATE + ", O_ORDERPRIORITY="
                + O_ORDERPRIORITY + ", O_CLERK=" + O_CLERK + ", O_SHIPPRIORITY=" + O_SHIPPRIORITY + ", O_COMMENT="
                + O_COMMENT + "]";
    }
}
