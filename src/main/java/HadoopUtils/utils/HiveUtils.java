package HadoopUtils.utils;

/**
 * Created by Meowcle~ on 2017/7/19.
 */
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

/**
 * Created by Meowcle~ on 2017/7/19.
 */
public class HiveUtils {

    private static String driverName = "org.apache.hive.jdbc.HiveDriver";
    private static String uri = "jdbc:hive2://localhost:10000/hive";
    private static String user = "Meowcle~";
    private static String password = "1234";
    private static String sql = "";
    private static ResultSet res;
    private static final Logger log = Logger.getLogger(HiveUtils.class);
    private static String tableName = "hivetesttable";

    public static void countData(Statement stmt, String tableName)
            throws SQLException {
        sql = "select count(1) from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        System.out.println("Query result: ");
        while (res.next()) {
            System.out.println("count ------>" + res.getString(1));
        }
    }

    public static void selectData(Statement stmt, String tableName)
            throws SQLException {
        sql = "select * from " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        System.out.println("Query result: ");
        while (res.next()) {
            System.out.println(res.getInt(1) + "\t" + res.getString(2));
        }
    }

    public static void describeTables(Statement stmt, String tableName)
            throws SQLException {
        sql = "describe " + tableName;
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        System.out.println("Query result: ");
        while (res.next()) {
            System.out.println(res.getString(1) + "\t" + res.getString(2));
        }
    }

    public static void showTables(Statement stmt, String tableName)
            throws SQLException {
        sql = "show tables '" + tableName + "'";
        System.out.println("Running: " + sql);
        res = stmt.executeQuery(sql);
        System.out.println("Query result: ");
        if (res.next()) {
            System.out.println(res.getString(1));
        }
    }

    public static void createTable(Statement stmt, String tableName)
            throws SQLException {
        sql = "create table " + tableName
                + " (key int, value string)  row format delimited fields terminated by '\t'";
        stmt.execute(sql);
    }

    public static boolean dropTable(Statement stmt, String tableName) throws SQLException {
        // 创建的表名
        sql = "drop table " + tableName;
        stmt.execute(sql);
        return true;
    }

    public static Connection getConn() throws ClassNotFoundException,
            SQLException {
        Class.forName(driverName);
        Connection conn = DriverManager.getConnection(uri, user, password);
        return conn;
    }

    public static void main(String[] args) {
        Connection conn = null;
        Statement stmt = null;
        try {
            conn = getConn();
            stmt = conn.createStatement();

            dropTable(stmt, tableName);
            createTable(stmt, tableName);
            showTables(stmt, tableName);
            describeTables(stmt, tableName);
            //loadData(stmt, tableName);
            selectData(stmt, tableName);
            countData(stmt, tableName);

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
            log.error(driverName + " not found!", e);
            System.exit(1);
        } catch (SQLException e) {
            e.printStackTrace();
            log.error("Connection error!", e);
            System.exit(1);
        } finally {
            try {
                if (conn != null) {
                    conn.close();
                    conn = null;
                }
                if (stmt != null) {
                    stmt.close();
                    stmt = null;
                }
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
}
