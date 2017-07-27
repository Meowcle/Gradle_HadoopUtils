package HadoopUtils; 

import HadoopUtils.utils.HiveUtils;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import java.sql.Connection;
import java.sql.Statement;

import static HadoopUtils.utils.HiveUtils.getConn;
import static org.junit.Assert.assertEquals;

/** 
* HiveUtils Tester. 
* 
* @author Meowcle~
* @since <pre>07/19/2017</pre> 
* @version 1.0 
*/ 
public class HiveUtilsTest {

    Connection conn = null;
    Statement stmt = null;
    public String tableName = "hivetesttable";

@Before
public void before() throws Exception {
    conn = getConn();
    stmt = conn.createStatement();
} 

@After
public void after() throws Exception { 
}

/** 
* 
* Method: countData(Statement stmt, String tableName) 
* 
*/ 
@Test
public void testCountData() throws Exception {
    try{
        HiveUtils.countData(stmt, tableName);
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: selectData(Statement stmt, String tableName) 
* 
*/ 
@Test
public void testSelectData() throws Exception {
    try{
        HiveUtils.selectData(stmt, tableName);
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: loadData(Statement stmt, String tableName) 
* 
*/ 
@Test
public void testLoadData() throws Exception {
} 

/** 
* 
* Method: describeTables(Statement stmt, String tableName) 
* 
*/ 
@Test
public void testDescribeTables() throws Exception {
    try{
        HiveUtils.describeTables(stmt, tableName);
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: showTables(Statement stmt, String tableName) 
* 
*/ 
@Test
public void testShowTables() throws Exception {
    try{
        HiveUtils.showTables(stmt, tableName);
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: createTable(Statement stmt, String tableName) 
* 
*/ 
@Test
public void testCreateTable() throws Exception {
    try{
        HiveUtils.createTable(stmt, tableName);
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: dropTable(Statement stmt) 
* 
*/ 
@Test
public void testDropTable() throws Exception {
    try{
        assertEquals(true, HiveUtils.dropTable(stmt, tableName));
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: getConn() 
* 
*/ 
@Test
public void testGetConn() throws Exception {
}

/**
 *
 * Method: main(String[] args)
 *
 */
@Test
public void testMain() throws Exception {
}
} 
