package tpc_query.Database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

public abstract class Table implements ITable {
    public String tableName;
    public boolean isRoot;
    public boolean isLeaf;
    public int numChild;
    public List<String> parents;
    public List<String> children;
    public Hashtable<Long, String> indexLiveTuple; // I(L(R)) leaf R, (key,value) = (pKey, live tuple)
    public Hashtable<Long, String> indexNonLiveTuple; // I(N(R)) non-leaf R, (key,value) = (pKey, non-live tuple)
    public Hashtable<Long, Integer> sCounter; // for non-leaf R, (key,value) = (pKey, num of child of this tuple is
                                              // alive)
    public Hashtable<String, HashMap<Long, ArrayList<Long>>> indexTableAndTableChildInfo; // (key, value) = childName,
                                                                                          // thisTableAndTableChildInfo

    public String toString() {
        return "This is a Table";
    }

}
