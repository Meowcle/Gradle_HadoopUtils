package HadoopUtils;

/**
 * Created by Meowcle~ on 2017/7/12.
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class HbaseUtils {
    // 声明静态配置
    static Configuration conf = null;
    private static boolean isConnectSuccess = false;
    private static HConnection conn;
    private static HTablePool tablePool;
    private static final int poolsize = 111;

    static {
        System.setProperty("hadoop.home.dir", "/");
        conf = HBaseConfiguration.create();
        conf.set("zookeeper.znode.parent", "/hbase-unsecure");
        conf.set("hbase.zookeeper.quorum", "dsjpt1441,dsjpt1442,dsjpt1443");
        // conf.set("hbase.zookeeper.quorum", "localhost");

        conf.set("hbase.zookeeper.property.clientPort", "2181");
        UserGroupInformation.setConfiguration(conf);
        try {
            tablePool = new HTablePool(conf, poolsize);
            conn = HConnectionManager.createConnection(conf);
            isConnectSuccess = true;
        } catch (IOException e) {
            e.printStackTrace();
            isConnectSuccess = false;
        }
    }

    /**
     *
     * 创建表
     *
     * @tableName 表名
     */
    public static void creatTable(String tableName)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(Bytes.toBytes(tableName))) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    /**
     *
     * 创建表
     *
     * @tableName 表名
     *
     * @family 列族名
     */
    public static void creatTable(String tableName, String family)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(Bytes.toBytes(tableName))) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            desc.addFamily(new HColumnDescriptor(family));
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    /**
     *
     * 创建表
     *
     * @tableName 表名
     *
     * @families 列族列表
     */
    public static void creatTable(String tableName, String[] families)
            throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(Bytes.toBytes(tableName))) {
            System.out.println("table Exists!");
            System.exit(0);
        } else {
            HTableDescriptor desc = new HTableDescriptor(tableName);
            for (int i = 0; i < families.length; i++) {
                desc.addFamily(new HColumnDescriptor(families[i]));
            }
            admin.createTable(desc);
            System.out.println("create table Success!");
        }
    }

    /**
     *
     * 初始化表
     *
     * @tableName 表名
     */
    public static void initTable(String tableName) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            deleteTable(tableName);
            creatTable(tableName);
        } else {
            creatTable(tableName);
        }
    }

    /**
     *
     * 初始化表
     *
     * @tableName 表名
     *
     * @family 列族名
     */
    public static void initTable(String tableName, String family) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            deleteTable(tableName);
            creatTable(tableName, family);
        } else {
            creatTable(tableName, family);
        }
    }

    /**
     *
     * 初始化表
     *
     * @tableName 表名
     *
     * @families 列族列表
     */
    public static void initTable(String tableName, String[] families) throws Exception {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (admin.tableExists(tableName)) {
            deleteTable(tableName);
            creatTable(tableName, families);
        } else {
            creatTable(tableName, families);
        }
    }

    /**
     *
     * 为表添加数据（适合知道有多少列族的固定表）
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     *
     * @column1 第一个列族列表
     *
     * @value1 第一个列的值的列表
     *
     * @column2 第二个列族列表
     *
     * @value2 第二个列的值的列表
     */
    public static void addData(String rowKey, String tableName,
                               String[] column1, String[] value1, String[] column2, String[] value2)
            throws IOException {

        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        Put put = new Put(Bytes.toBytes(rowKey));// 设置rowkey
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// HTabel负责跟记录相关的操作如增删改查等//
        // 获取表
        HColumnDescriptor[] columnFamilies = table.getTableDescriptor() // 获取所有的列族
                .getColumnFamilies();

        for (int i = 0; i < columnFamilies.length; i++) {
            String familyName = columnFamilies[i].getNameAsString(); // 获取列族名
            if (familyName.equals("article")) { // article列族put数据
                for (int j = 0; j < column1.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column1[j]), Bytes.toBytes(value1[j]));
                }
            }
            if (familyName.equals("author")) { // author列族put数据
                for (int j = 0; j < column2.length; j++) {
                    put.add(Bytes.toBytes(familyName),
                            Bytes.toBytes(column2[j]), Bytes.toBytes(value2[j]));
                }
            }
        }
        table.put(put);
        System.out.println("add data Success!");
    }

    /**
     *
     * 根据rwokey查询
     *
     * @rowKey rowKey
     *
     * @tableName 表名
     */
    public static Result getResult(String tableName, String rowKey)
            throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        Get get = new Get(Bytes.toBytes(rowKey));
        HTable table = new HTable(conf, Bytes.toBytes(tableName));// 获取表
        Result result = table.get(get);
        OutputWithoutRow(result);
        return result;
    }

    /**
     *
     * 遍历查询hbase表
     *
     * @tableName 表名
     */
    public static void getResultScann(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        Scan scan = new Scan();
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            OutputWithRow(rs);
        } finally {
            rs.close();
        }
    }

    /**
     *
     * 根据row key范围遍历查询hbase表
     *
     * @tableName 表名
     *
     * @start_rowkey 起始row key
     *
     * @stop_rowkey 终止row key
     */
    public static void getResultScann(String tableName, String start_rowkey,
                                      String stop_rowkey) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        Scan scan = new Scan();
        scan.setStartRow(Bytes.toBytes(start_rowkey));
        scan.setStopRow(Bytes.toBytes(stop_rowkey));
        ResultScanner rs = null;
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        try {
            rs = table.getScanner(scan);
            OutputWithRow(rs);
        } finally {
            rs.close();
        }
    }

    /**
     *
     * 查询表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public static void getResultByColumn(String tableName, String rowKey,
                                         String familyName, String columnName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName)); // 获取指定列族和列修饰符对应的列
        Result result = table.get(get);
        OutputWithoutRow(result);
    }

    /**
     *
     * 更新表中的某一列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     *
     * @value 更新后的值
     */
    public static void updateTable(String tableName, String rowKey,
                                   String familyName, String columnName, String value)
            throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Put put = new Put(Bytes.toBytes(rowKey));
        put.add(Bytes.toBytes(familyName), Bytes.toBytes(columnName),
                Bytes.toBytes(value));
        table.put(put);
        System.out.println("update table Success!");
    }

    /**
     *
     * 查询某列数据的多个版本
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public static void getResultByVersion(String tableName, String rowKey,
                                          String familyName, String columnName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Get get = new Get(Bytes.toBytes(rowKey));
        get.addColumn(Bytes.toBytes(familyName), Bytes.toBytes(columnName));
        get.setMaxVersions(5);
        Result result = table.get(get);
        OutputWithoutRow(result);
    }

    /**
     *
     * 根据rowKey删除记录
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public static void deleteRecordByRowKey(String tableName, String rowKey)throws Exception {
        HTableInterface table = tablePool.getTable(tableName);
        try {
            List<Delete> list = new ArrayList<Delete>();
            Delete d1 = new Delete(Bytes.toBytes(rowKey));
            list.add(d1);
            table.delete(list);
            System.out.println("record with rowKey: \' " + rowKey + " \' is deleted from table  " + tableName+ " !");
        } catch (Exception e) {
            System.err.println("deleteRecordByRowKey exception" + e);
            throw e;
        } finally{
            table.close();
        }
    }

    /**
     *
     * 删除指定的列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     *
     * @familyName 列族名
     *
     * @columnName 列名
     */
    public static void deleteColumn(String tableName, String rowKey,
                                    String falilyName, String columnName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteColumn = new Delete(Bytes.toBytes(rowKey));
        deleteColumn.deleteColumns(Bytes.toBytes(falilyName),
                Bytes.toBytes(columnName));
        table.delete(deleteColumn);
        System.out.println(falilyName + ":" + columnName + "is deleted!");
    }

    /**
     *
     * 删除指定的列
     *
     * @tableName 表名
     *
     * @rowKey rowKey
     */
    public static void deleteAllColumn(String tableName, String rowKey)
            throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        HTable table = new HTable(conf, Bytes.toBytes(tableName));
        Delete deleteAll = new Delete(Bytes.toBytes(rowKey));
        table.delete(deleteAll);
        System.out.println("all columns are deleted!");
    }

    /**
     *
     * 删除表
     *
     * @tableName 表名
     */
    public static void deleteTable(String tableName) throws IOException {
        HBaseAdmin admin = new HBaseAdmin(conf);
        if (!admin.tableExists(Bytes.toBytes(tableName))) {
            System.err.println("table " + tableName + " DOES NOT exist");
            System.exit(1);
        }
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
        System.out.println(tableName + " is deleted!");
    }

    /**
     *
     * Duplicated code for output records.
     *
     * @result 查询结果列表
     */
    public static void OutputWithoutRow(Result result) throws IOException {
        for (KeyValue kv : result.list()) {
            System.out.println("family:" + Bytes.toString(kv.getFamily()));
            System.out.println("qualifier:" + Bytes.toString(kv.getQualifier()));
            System.out.println("value:" + Bytes.toString(kv.getValue()));
            System.out.println("Timestamp:" + kv.getTimestamp());
            System.out.println("-------------------------------------------");
        }
    }

    /**
     *
     * Duplicated code for output records.
     *
     * @rs 查询结果列表
     */
    public static void OutputWithRow(ResultScanner rs) throws IOException {
        for (Result r : rs) {
            for (KeyValue kv : r.list()) {
                System.out.println("row:" + Bytes.toString(kv.getRow()));
                System.out.println("family:"
                        + Bytes.toString(kv.getFamily()));
                System.out.println("qualifier:"
                        + Bytes.toString(kv.getQualifier()));
                System.out
                        .println("value:" + Bytes.toString(kv.getValue()));
                System.out.println("timestamp:" + kv.getTimestamp());
                System.out
                        .println("-------------------------------------------");
            }
        }
    }

    public static void main(String[] args) throws Exception {

        // 创建表
        String tableName = "blog2017";
        String[] families = { "article", "author" };
        creatTable(tableName, families);

        // 为表添加数据
        String[] column1 = { "title", "content", "tag" };
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };
        addData("rowkey1", "blog2017", column1, value1, column2, value2);
        addData("rowkey2", "blog2017", column1, value1, column2, value2);
        addData("rowkey3", "blog2017", column1, value1, column2, value2);

        // 遍历查询
        getResultScann("blog2017");
        // 根据row key范围遍历查询
        getResultScann("blog2017", "rowkey4", "rowkey5");

        // 查询
        getResult("blog2017", "rowkey1");

        // 查询某一列的值
        getResultByColumn("blog2017", "rowkey1", "author", "name");

        // 更新列
        updateTable("blog2017", "rowkey1", "author", "name", "bin");

        // 查询某一列的值
        getResultByColumn("blog2017", "rowkey1", "author", "name");

        // 查询某列的多版本
        getResultByVersion("blog2017", "rowkey1", "author", "name");

        // 初始化表
        initTable(tableName, families);

        // 重新为表添加数据
        addData("rowkey1", "blog2017", column1, value1, column2, value2);
        addData("rowkey2", "blog2017", column1, value1, column2, value2);
        addData("rowkey3", "blog2017", column1, value1, column2, value2);

        // 遍历查询
        getResultScann("blog2017");

        // 删除一条记录
        deleteRecordByRowKey("blog2017", "rowkey1");
        getResultScann("blog2017");

        // 删除一列
        deleteColumn("blog2017", "rowkey1", "author", "nickname");
        getResultScann("blog2017");

        // 删除所有列
        deleteAllColumn("blog2017", "rowkey1");
        getResultScann("blog2017");

        // 删除表
        deleteTable("blog2017");

    }
}
