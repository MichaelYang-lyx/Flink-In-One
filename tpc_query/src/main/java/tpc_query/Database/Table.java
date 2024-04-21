package tpc_query.Database;

import java.util.List;

public abstract class Table implements ITable {
    public String tableName;
    public boolean isRoot;
    public boolean isLeaf;
    public int numChild;
    public List<String> parents;
    public List<String> children;

    public String toString() {
        return "This is a Table";
    }

}
