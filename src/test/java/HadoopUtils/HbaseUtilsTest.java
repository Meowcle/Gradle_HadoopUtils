package HadoopUtils;

import HadoopUtils.utils.HbaseUtils;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* HbaseUtils Tester.
* 
* @author Meowcle~
* @since <pre>07/12/2017</pre> 
* @version 1.0 
*/ 
public class HbaseUtilsTest {
    public static HbaseUtils hbaseUtils;

    @Before
    public void before() throws Exception {
        hbaseUtils = new HbaseUtils();
    }

    @After
    public void after() throws Exception {
    }

    /**
     *
     * Method: listTable()
     *
     */
    @Test
    public void testListTable() throws Exception {
        hbaseUtils.listTable();
    }

    /**
     *
     * Method: listTableNames()
     *
     */
    @Test
    public void testListTableNames() throws Exception {
        hbaseUtils.listTableNames();
    }

    /**
    *
    * Method: creatTable(String tableName, String[] family)
    *
    */
    @Test
    public void testCreatTable() throws Exception {
        String tableName = "blog2";
        String[] family = { "article", "author" };
        hbaseUtils.creatTable(tableName, family);
    }

    /**
    *
    * Method: addData(String rowKey, String tableName, String[] column1, String[] value1, String[] column2, String[] value2)
    *
    */
    @Test
    public void testAddData() throws Exception {
        String[] column1 = { "title", "content", "tag" };
        String[] value1 = {
                "Head First HBase",
                "HBase is the Hadoop database. Use it when you need random, realtime read/write access to your Big Data.",
                "Hadoop,HBase,NoSQL" };
        String[] column2 = { "name", "nickname" };
        String[] value2 = { "nicholas", "lee" };
        hbaseUtils.addData("rowkey1", "blog2", column1, value1, column2, value2);
        hbaseUtils.addData("rowkey2", "blog2", column1, value1, column2, value2);
        hbaseUtils.addData("rowkey3", "blog2", column1, value1, column2, value2);
    }

    /**
     *
     * Method: getRowkeyList()
     *
     */
    @Test
    public void getRowkeyList() throws Exception {
        hbaseUtils.getRowkeyList("blog2017");
    }

    /**
    *
    * Method: getResult(String tableName, String rowKey)
    *
    */
    @Test
    public void testGetResult() throws Exception {
        hbaseUtils.getResult("blog2", "rowkey1");
    }

    /**
    *
    * Method: getResultScann(String tableName)
    *
    */
    @Test
    public void testGetResultScannTableName() throws Exception {
        hbaseUtils.getResultScann("blog2017");
    }

    /**
    *
    * Method: getResultScann(String tableName, String start_rowkey, String stop_rowkey)
    *
    */
    @Test
    public void testGetResultScannForTableNameStart_rowkeyStop_rowkey() throws Exception {
        hbaseUtils.getResultScann("blog2", "rowkey4", "rowkey5");
    }

    /**
    *
    * Method: getResultByColumn(String tableName, String rowKey, String familyName, String columnName)
    *
    */
    @Test
    public void testGetResultByColumn() throws Exception {
        hbaseUtils.getResultByColumn("blog2", "rowkey1", "author", "name");
    }

    /**
    *
    * Method: updateTable(String tableName, String rowKey, String familyName, String columnName, String value)
    *
    */
    @Test
    public void testUpdateTable() throws Exception {
        System.out.println("Before update.");
        hbaseUtils.updateTable("blog2", "rowkey1", "author", "name", "bin");
        System.out.println("After update.");
        hbaseUtils.getResultByColumn("blog2", "rowkey1", "author", "name");
    }

    /**
    *
    * Method: getResultByVersion(String tableName, String rowKey, String familyName, String columnName)
    *
    */
    @Test
    public void testGetResultByVersion() throws Exception {
        hbaseUtils.getResultByVersion("blog2", "rowkey1", "author", "name");
    }

    /**
    *
    * Method: deleteColumn(String tableName, String rowKey, String falilyName, String columnName)
    *
    */
    @Test
    public void testDeleteColumn() throws Exception {
        hbaseUtils.deleteColumn("blog2", "rowkey1", "author", "nickname");
    }

    /**
    *
    * Method: deleteAllColumn(String tableName, String rowKey)
    *
    */
    @Test
    public void testDeleteAllColumn() throws Exception {
        hbaseUtils.deleteAllColumn("blog2", "rowkey1");
    }

    /**
    *
    * Method: deleteTable(String tableName)
    *
    */
    @Test
    public void testDeleteTable() throws Exception {
        hbaseUtils.deleteTable("blog2017");
    }
} 
