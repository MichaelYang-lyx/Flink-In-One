
package tpc_query;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.io.File;
import java.sql.Date;
import java.util.Objects;
import java.util.Scanner;

import Tables.*;
import tpc_query.Update.Update;
public class TPCSource implements SourceFunction<Update>{
    boolean running = true;
    @Override
    public void run(SourceContext<Update> sourceData) throws Exception {
        Scanner sc = new Scanner(new File("data/source_data.csv"));

        while (running) {
            String[] line = sc.nextLine().split("\\|");
            String operation = line[0];
            String tableName = line[1];
            if (tableName.compareTo("lineitem.tbl") == 0) {
                Lineitem tuple = new Lineitem(Long.valueOf(line[0+2]), Long.valueOf(line[2 + 2]), Long.valueOf(line[3 + 2]), Double.valueOf(line[5 + 2]), Double.valueOf(line[6 + 2]), Date.valueOf(line[10+2]));
                sourceData.collect(new Update(operation, "Lineitem", tuple));

            } else if (tableName.compareTo("orders.tbl") == 0) {
                Orders tuple = new Orders(Long.valueOf(line[0 + 2]), Long.valueOf(line[1 + 2]));
                sourceData.collect(new Update(operation, "Orders", tuple));

            } else if (tableName.compareTo("customer.tbl") == 0) {
                Customer tuple = new Customer(Long.valueOf(line[0 + 2]), Long.valueOf(line[3 + 2]));
                sourceData.collect(new Update(operation, "Customer", tuple));

            } else if (tableName.compareTo("supplier.tbl") == 0) {
                Supplier tuple = new Supplier(Long.valueOf(line[0 + 2]), Long.valueOf(line[3 + 2]));
                sourceData.collect(new Update(operation, "Supplier", tuple));

            } else  {
                Nation tuple = new Nation(Long.valueOf(line[0 + 2]), line[1 + 2]);
                sourceData.collect(new Update(operation, "Nation1", tuple));
                sourceData.collect(new Update(operation, "Nation2", tuple));

            }
//            Thread.sleep(10);
            if (!sc.hasNext()) {
                System.out.println("finished");
                running = false;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

}