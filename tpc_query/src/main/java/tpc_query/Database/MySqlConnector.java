package tpc_query.Database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class MySqlConnector {
    public MySqlConnector() {
    }

    public static String MYSQL_URL = "jdbc:mysql://localhost:3306/tpc_h";
    public static String MYSQL_USER = "root";
    public static String MYSQL_PASSWORD = "";

    public static void main(String[] args) throws ClassNotFoundException, SQLException {//
        // createTableTest();
        clearTPCHData();
        // createTPCHTable();

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
        String sql = "insert into stu(id,name,age) values(?,?,?)";
        // 4.获取预处理对象，并依次给参数赋值
        PreparedStatement statement = connection.prepareCall(sql);
        statement.setInt(1, 12); // 数据库字段类型是int，就是setInt；1代表第一个参数
        statement.setString(2, "小明"); // 数据库字段类型是String，就是setString；2代表第二个参数
        statement.setInt(3, 16); // 数据库字段类型是int，就是setInt；3代表第三个参数
        // 5.执行sql语句（执行了几条记录，就返回几）
        int i = statement.executeUpdate();
        System.out.println(i);
        // 6.关闭jdbc连接
        statement.close();
        connection.close();
    }

    public static void createTableTest() throws ClassNotFoundException, SQLException {

        Class.forName("com.mysql.jdbc.Driver");
        Connection connection = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD);
        String sql = "CREATE TABLE IF NOT EXISTS stu (id INT, name VARCHAR(255), age INT)";
        PreparedStatement statement = connection.prepareCall(sql);
        int i = statement.executeUpdate();
        System.out.println(i + " finish create table");
        statement.close();
        connection.close();
    }

    public static void clearTPCHData() {

        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            Statement stmt = conn.createStatement();

            String[] tableNames = { "LINEITEM", "REGION", "NATION", "PART", "SUPPLIER", "PARTSUPP", "CUSTOMER",
                    "ORDERS" };

            for (String tableName : tableNames) {
                stmt.executeUpdate("DELETE FROM " + tableName);
            }

            System.out.println("Data cleared from all tables successfully.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createTPCHTable() throws ClassNotFoundException, SQLException {

        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            Statement stmt = conn.createStatement();

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS REGION (" +
                            "R_REGIONKEY INT PRIMARY KEY," +
                            "R_NAME VARCHAR(25)," +
                            "R_COMMENT VARCHAR(152)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS NATION (" +
                            "N_NATIONKEY INT PRIMARY KEY," +
                            "N_NAME VARCHAR(25)," +
                            "N_REGIONKEY INT," +
                            "N_COMMENT VARCHAR(152)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS PART (" +
                            "P_PARTKEY INT PRIMARY KEY," +
                            "P_NAME VARCHAR(55)," +
                            "P_MFGR CHAR(25)," +
                            "P_BRAND CHAR(10)," +
                            "P_TYPE VARCHAR(25)," +
                            "P_SIZE INT," +
                            "P_CONTAINER CHAR(10)," +
                            "P_RETAILPRICE DECIMAL(15,2)," +
                            "P_COMMENT VARCHAR(23)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS SUPPLIER (" +
                            "S_SUPPKEY INT PRIMARY KEY," +
                            "S_NAME CHAR(25)," +
                            "S_ADDRESS VARCHAR(40)," +
                            "S_NATIONKEY INT," +
                            "S_PHONE CHAR(15)," +
                            "S_ACCTBAL DECIMAL(15,2)," +
                            "S_COMMENT VARCHAR(101)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS PARTSUPP (" +
                            "PS_PARTKEY INT," +
                            "PS_SUPPKEY INT," +
                            "PS_AVAILQTY INT," +
                            "PS_SUPPLYCOST DECIMAL(15,2)," +
                            "PS_COMMENT VARCHAR(199)," +
                            "PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS CUSTOMER (" +
                            "C_CUSTKEY INT PRIMARY KEY," +
                            "C_NAME VARCHAR(25)," +
                            "C_ADDRESS VARCHAR(40)," +
                            "C_NATIONKEY INT," +
                            "C_PHONE CHAR(15)," +
                            "C_ACCTBAL DECIMAL(15,2)," +
                            "C_MKTSEGMENT CHAR(10)," +
                            "C_COMMENT VARCHAR(117)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS ORDERS (" +
                            "O_ORDERKEY INT PRIMARY KEY," +
                            "O_CUSTKEY INT," +
                            "O_ORDERSTATUS CHAR(1)," +
                            "O_TOTALPRICE DECIMAL(15,2)," +
                            "O_ORDERDATE DATE," +
                            "O_ORDERPRIORITY CHAR(15)," +
                            "O_CLERK CHAR(15)," +
                            "O_SHIPPRIORITY INT," +
                            "O_COMMENT VARCHAR(79)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS LINEITEM (" +
                            "L_ORDERKEY INT," +
                            "L_PARTKEY INT," +
                            "L_SUPPKEY INT," +
                            "L_LINENUMBER INT," +
                            "L_QUANTITY DECIMAL(15,2)," +
                            "L_EXTENDEDPRICE DECIMAL(15,2)," +
                            "L_DISCOUNT DECIMAL(15,2)," +
                            "L_TAX DECIMAL(15,2)," +
                            "L_RETURNFLAG CHAR(1)," +
                            "L_LINESTATUS CHAR(1)," +
                            "L_SHIPDATE DATE," +
                            "L_COMMITDATE DATE," +
                            "L_RECEIPTDATE DATE," +
                            "L_SHIPINSTRUCT CHAR(25)," +
                            "L_SHIPMODE CHAR(10)," +
                            "L_COMMENT VARCHAR(44)," +
                            "PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)" +
                            ");");

            System.out.println("Tables created successfully.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void createHardTPCHTable() throws ClassNotFoundException, SQLException {

        try (Connection conn = DriverManager.getConnection(MYSQL_URL, MYSQL_USER, MYSQL_PASSWORD)) {
            Statement stmt = conn.createStatement();

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS REGION (" +
                            "R_REGIONKEY INT PRIMARY KEY," +
                            "R_NAME VARCHAR(25)," +
                            "R_COMMENT VARCHAR(152)" +
                            ");");
            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS NATION (" +
                            "N_NATIONKEY INT PRIMARY KEY," +
                            "N_NAME VARCHAR(25)," +
                            "N_REGIONKEY INT," +
                            "N_COMMENT VARCHAR(152)," +
                            "FOREIGN KEY (N_REGIONKEY) REFERENCES REGION(R_REGIONKEY)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS PART (" +
                            "P_PARTKEY INT PRIMARY KEY," +
                            "P_NAME VARCHAR(55)," +
                            "P_MFGR CHAR(25)," +
                            "P_BRAND CHAR(10)," +
                            "P_TYPE VARCHAR(25)," +
                            "P_SIZE INT," +
                            "P_CONTAINER CHAR(10)," +
                            "P_RETAILPRICE DECIMAL(15,2)," +
                            "P_COMMENT VARCHAR(23)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS SUPPLIER (" +
                            "S_SUPPKEY INT PRIMARY KEY," +
                            "S_NAME CHAR(25)," +
                            "S_ADDRESS VARCHAR(40)," +
                            "S_NATIONKEY INT," +
                            "S_PHONE CHAR(15)," +
                            "S_ACCTBAL DECIMAL(15,2)," +
                            "S_COMMENT VARCHAR(101)," +
                            "FOREIGN KEY (S_NATIONKEY) REFERENCES NATION(N_NATIONKEY)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS PARTSUPP (" +
                            "PS_PARTKEY INT," +
                            "PS_SUPPKEY INT," +
                            "PS_AVAILQTY INT," +
                            "PS_SUPPLYCOST DECIMAL(15,2)," +
                            "PS_COMMENT VARCHAR(199)," +
                            "PRIMARY KEY (PS_PARTKEY, PS_SUPPKEY)," +
                            "FOREIGN KEY (PS_PARTKEY) REFERENCES PART(P_PARTKEY)," +
                            "FOREIGN KEY (PS_SUPPKEY) REFERENCES SUPPLIER(S_SUPPKEY)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS CUSTOMER (" +
                            "C_CUSTKEY INT PRIMARY KEY," +
                            "C_NAME VARCHAR(25)," +
                            "C_ADDRESS VARCHAR(40)," +
                            "C_NATIONKEY INT," +
                            "C_PHONE CHAR(15)," +
                            "C_ACCTBAL DECIMAL(15,2)," +
                            "C_MKTSEGMENT CHAR(10)," +
                            "C_COMMENT VARCHAR(117)," +
                            "FOREIGN KEY (C_NATIONKEY) REFERENCES NATION(N_NATIONKEY)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS ORDERS (" +
                            "O_ORDERKEY INT PRIMARY KEY," +
                            "O_CUSTKEY INT," +
                            "O_ORDERSTATUS CHAR(1)," +
                            "O_TOTALPRICE DECIMAL(15,2)," +
                            "O_ORDERDATE DATE," +
                            "O_ORDERPRIORITY CHAR(15)," +
                            "O_CLERK CHAR(15)," +
                            "O_SHIPPRIORITY INT," +
                            "O_COMMENT VARCHAR(79)," +
                            "FOREIGN KEY (O_CUSTKEY) REFERENCES CUSTOMER(C_CUSTKEY)" +
                            ");");

            stmt.executeUpdate(
                    "CREATE TABLE IF NOT EXISTS LINEITEM (" +
                            "L_ORDERKEY INT," +
                            "L_PARTKEY INT," +
                            "L_SUPPKEY INT," +
                            "L_LINENUMBER INT," +
                            "L_QUANTITY DECIMAL(15,2)," +
                            "L_EXTENDEDPRICE DECIMAL(15,2)," +
                            "L_DISCOUNT DECIMAL(15,2)," +
                            "L_TAX DECIMAL(15,2)," +
                            "L_RETURNFLAG CHAR(1)," +
                            "L_LINESTATUS CHAR(1)," +
                            "L_SHIPDATE DATE," +
                            "L_COMMITDATE DATE," +
                            "L_RECEIPTDATE DATE," +
                            "L_SHIPINSTRUCT CHAR(25)," +
                            "L_SHIPMODE CHAR(10)," +
                            "L_COMMENT VARCHAR(44)," +
                            "PRIMARY KEY (L_ORDERKEY, L_LINENUMBER)," +
                            "FOREIGN KEY (L_ORDERKEY) REFERENCES ORDERS(O_ORDERKEY)," +
                            "FOREIGN KEY (L_PARTKEY, L_SUPPKEY) REFERENCES PARTSUPP(PS_PARTKEY, PS_SUPPKEY)" +
                            ");");

            System.out.println("Tables created successfully.");

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
