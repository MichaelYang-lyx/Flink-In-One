package tpc_query.Database;

import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import tpc_query.DataStream.DataOperation;
import tpc_query.DataStream.DataContent.IDataContent;
import tpc_query.Query.IQuery;
import tpc_query.Query.Q7;
import tpc_query.Update.Delete;
import tpc_query.Update.Insert;
import java.io.File;

public class MemorySink extends RichSinkFunction<DataOperation> {

    private MapState<Long, Tuple4<String, String, Integer, Double>> joinResultState;
    public TableController tableController;

    public Map<String, ITable> tables;

    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("@@@@@@ MemorySink open @@@@@@");
        super.open(parameters);

        joinResultState = getRuntimeContext().getMapState(
                new MapStateDescriptor<>("ResultState", Types.LONG,
                        Types.TUPLE(Types.STRING, Types.STRING, Types.DOUBLE, Types.DOUBLE)));

        tableController = new TableController("MySQL");
        IQuery query = new Q7();
        tableController.setupTables(query);
        tables = tableController.tables;

    }

    @Override
    public void close() throws Exception {

        try {
            for (Map.Entry<String, ITable> entry : tables.entrySet()) {
                MyTable table = (MyTable) entry.getValue();
                PrintWriter writer = new PrintWriter(new File("output/tpc_query/" + entry.getKey() + ".txt"));
                writer.println("-------- Table Name: " + entry.getKey() + "---------");
                for (Map.Entry<Long, IDataContent> tupleEntry : table.allTuples.entrySet()) {
                    writer.println("Primary Key: " + tupleEntry.getKey());
                    writer.println("Data Content: " + tupleEntry.getValue());
                }
                writer.close();
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }

        System.out.println("==-------- Q7.directSelect1 ---------==");
        System.out.println(Q7.directSelect1(tables));

        System.out.println("==-------- joinResult) ---------==");
        Iterable<Map.Entry<Long, Tuple4<String, String, Integer, Double>>> entries = joinResultState.entries();
        for (Map.Entry<Long, Tuple4<String, String, Integer, Double>> entry : entries) {
            Long key = entry.getKey();
            Tuple4<String, String, Integer, Double> value = entry.getValue();
            System.out.println("Key: " + key + ", Value: " + value);
        }

        super.close();
    }

    @Override
    public void invoke(DataOperation dataOperation, Context context) throws Exception {
        // below for test
        if (dataOperation.operation.equals("+")) {

            if (dataOperation.tableName.equals("NATION")) {
                dataOperation.switchTableName("NATION1");
                Insert.insert(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
                dataOperation.switchTableName("NATION2");
                Insert.insert(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
                dataOperation.switchTableName("NATION");

            } else {
                Insert.insert(tables, dataOperation.tableName, dataOperation.dataContent, joinResultState);
            }
        } else {
            if (dataOperation.tableName.equals("NATION")) {
                dataOperation.switchTableName("NATION1");
                Delete.delete(tables, dataOperation.tableName, dataOperation.dataContent,
                        joinResultState);
                dataOperation.switchTableName("NATION2");
                Delete.delete(tables, dataOperation.tableName, dataOperation.dataContent,
                        joinResultState);
                dataOperation.switchTableName("NATION");

            } else {
                Delete.delete(tables, dataOperation.tableName, dataOperation.dataContent,
                        joinResultState);
            }

        }

    }
}
