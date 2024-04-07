package tpc_query.DataStream;

import java.util.Arrays;
import java.util.List;

import tpc_query.DataStream.DataContent.IDataContent;

public class DataOperation {

    public String operation;
    public String tableName;
    public IDataContent dataContent;

    public DataOperation(String operation, String tableName) {
        this.operation = operation;
        this.tableName = tableName;
        this.dataContent = null;
    }

    public DataOperation(String operation, String tableName, IDataContent dataContent) {
        this.operation = operation;
        this.tableName = tableName;
        this.dataContent = dataContent;
    }

    public String getOperation() {
        return operation;
    }

    public String getTableName() {
        return tableName;
    }

    public IDataContent getDataContent() {
        return dataContent;
    }

    public List<String> getContentList() {
        return dataContent.toList();
    }

    public String toString() {
        return "[ " + operation + "  " + tableName + " ] " +
                dataContent;
    }

}
