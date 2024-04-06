package tpc_query.Table;

public interface ITable {
    void insertRow(String data);

    String getRow(int id);

    void updateRow(int id, String newData);

    void deleteRow(int id);

    String toString();
}
