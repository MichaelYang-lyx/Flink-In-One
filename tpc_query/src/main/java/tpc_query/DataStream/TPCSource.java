
package tpc_query.DataStream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import tpc_query.DataStream.DataContent.*;

import java.io.File;
import java.util.Arrays;
import java.util.Scanner;

public class TPCSource implements SourceFunction<DataOperation> {
    boolean running = true;
    int HOLD_SIZE = 2;

    @Override
    public void run(SourceContext<DataOperation> sourceData) throws Exception {
        Scanner sc = new Scanner(new File("DBGEN/data/source_data.csv"));

        while (running) {
            String[] line = sc.nextLine().split("\\|");
            String operation = line[0];
            String tableName = line[1];
            IDataContent dataContent = null;
            if (tableName.equals("customer.tbl")) {
                dataContent = new Customer(Arrays.copyOfRange(line, HOLD_SIZE, line.length));

            } else if (tableName.equals("orders.tbl")) {
                dataContent = new Orders(Arrays.copyOfRange(line, HOLD_SIZE, line.length));

            } else if (tableName.equals("lineitem.tbl")) {
                dataContent = new LineItem(Arrays.copyOfRange(line, HOLD_SIZE, line.length));

            } else if (tableName.equals("supplier.tbl")) {
                dataContent = new Supplier(Arrays.copyOfRange(line, HOLD_SIZE, line.length));

            } else if (tableName.equals("nation.tbl")) {
                dataContent = new Nation(Arrays.copyOfRange(line, HOLD_SIZE, line.length));

            } else if (tableName.equals("region.tbl")) {
                dataContent = new Region(Arrays.copyOfRange(line, HOLD_SIZE, line.length));

            }
            tableName = tableName.replace(".tbl", "").toUpperCase();
            sourceData.collect(new DataOperation(operation, tableName, dataContent));

            if (!sc.hasNext()) {
                System.out.println("All data has been recieved.");
                running = false;
            }
        }
        sc.close();
    }

    @Override
    public void cancel() {
        running = false;
    }

}