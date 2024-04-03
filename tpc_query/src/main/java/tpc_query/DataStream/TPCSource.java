
package tpc_query.DataStream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import tpc_query.DataStream.DataContent.Customer;
import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.DataStream.DataContent.Orders;

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

            if (tableName.equals("customer.tbl")) {
                IDataContent dataContent = new Customer(Arrays.copyOfRange(line, HOLD_SIZE, line.length));
                sourceData.collect(new DataOperation(operation, tableName, dataContent));
            } else if (tableName.equals("orders.tbl")) {
                IDataContent dataContent = new Orders(Arrays.copyOfRange(line, HOLD_SIZE, line.length));
                sourceData.collect(new DataOperation(operation, tableName, dataContent));
            } else if (tableName.equals("lineitem.tbl")) {
            } else if (tableName.equals("supplier.tbl")) {
            } else if (tableName.equals("nation.tbl")) {
            } else if (tableName.equals("region.tbl")) {
            }

            sourceData.collect(new DataOperation(operation, tableName));
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