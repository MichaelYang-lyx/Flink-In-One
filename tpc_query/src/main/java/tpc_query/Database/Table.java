package tpc_query.Database;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.List;

import tpc_query.DataStream.DataContent.DataContent;
import tpc_query.DataStream.DataContent.IDataContent;

public abstract class Table implements ITable {
    public String tableName;
    public boolean isRoot;
    public boolean isLeaf;
    public int numChild;
    public List<String> parents;
    public List<String> children;
    public Hashtable<Long, IDataContent> indexLiveTuple; // I(L(R)) leaf R, (key,value) = (pKey, live tuple)
    public Hashtable<Long, IDataContent> indexNonLiveTuple; // I(N(R)) non-leaf R, (key,value) = (pKey, non-live tuple)
    public Hashtable<Long, Integer> sCounter; // for non-leaf R, (key,value) = (pKey, num of child of this tuple is
                                              // alive)
    public Hashtable<String, HashMap<Long, ArrayList<Long>>> indexTableAndTableChildInfo; // (key, value) = childName,
                                                                                          // thisTableAndTableChildInfo

    public String toString() {
        return "This is a Table: " + tableName;
    }

}
