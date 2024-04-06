package tpc_query.Table;

import java.util.ArrayList;

public abstract class Table implements ITable {
    public String tableName;
    public boolean isRoot;
    public boolean isLeaf;
    public int numChild;
    public ArrayList<String> childs;

    public String toString() {
        return "This is a Table";
    }

}
