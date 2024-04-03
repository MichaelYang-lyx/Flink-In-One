package tpc_query.Query;

import java.io.Serializable;

import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.DataContent.Orders;
import tpc_query.DataStream.DataContent.Region;

public class Q5 implements IQuery, Serializable {

    public boolean filter(DataOperation data) {
        if (data.getTableName().equals("region.tbl")) {
            Region region = (Region) data.getDataContent();
            return region.getR_NAME().equals("ASIA");
        } else if (data.getTableName().equals("orders.tbl")) {
            Orders orders = (Orders) data.getDataContent();
            return orders.getO_ORDERDATE().compareTo("1994-01-01") >= 0
                    && orders.getO_ORDERDATE().compareTo("1995-01-01") <= 0;
        }
        return true;
    }

}
