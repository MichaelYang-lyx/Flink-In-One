package tpc_query.DataStream.DataContent;

import java.util.HashMap;
import java.util.Arrays;

public class Customer implements IDataContent {
    public Long c_custkey;
    public Long c_nationkey;
    public String[] all;

    public Customer() {
    }

    public Customer(String[] string) {
        this.all = string;
    }

    public HashMap<String, Long> getForeignKey() {
        HashMap<String, Long> foreignKeyMapping = new HashMap<String, Long>();
        foreignKeyMapping.put("Nation2", c_nationkey);
        return foreignKeyMapping;
    }

    public String toString() {
        return "Customer [" + Arrays.toString(this.all) + "]";
    }
}
