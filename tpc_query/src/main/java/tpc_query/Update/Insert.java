package tpc_query.Update;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.List;

import tpc_query.Database.ITable;
import tpc_query.Database.MemoryTable;
import tpc_query.Database.Table;
import tpc_query.Database.TableController;
import tpc_query.Query.IQuery;
import tpc_query.Query.Q5;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.util.Collector;

public class Insert extends Update {
    public void run() {
        System.out.println("Insert");
    }

    public void main() {
        IQuery query = new Q5();
        TableController tableController = new TableController("memory");
        tableController.setupTables(query);
        MemoryTable table = (MemoryTable) tableController.tables.get("Customer");
        insert(table);
    }

    public void insert(Table table) {
        if (!table.isLeaf) {
            for (String childName : table.children) {
                System.out.println(childName);
            }

        }
    };

}
