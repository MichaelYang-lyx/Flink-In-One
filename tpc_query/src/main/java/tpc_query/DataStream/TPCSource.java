
package tpc_query.DataStream;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import tpc_query.DataStream.DataContent.Customer;

import java.io.File;
import java.util.Scanner;

public class TPCSource implements SourceFunction<DataOperation> {
    boolean running = true;

    @Override
    public void run(SourceContext<DataOperation> sourceData) throws Exception {
        Scanner sc = new Scanner(new File("DBGEN/data/source_data.csv"));

        while (running) {
            String[] line = sc.nextLine().split("\\|");
            String operation = line[0];
            String tableName = line[1];
            if (tableName.equals("customer.tbl")) {
                sourceData.collect(new DataOperation(operation, tableName, new Customer(line)));

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