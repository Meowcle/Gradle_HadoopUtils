package HadoopUtils.utils; 

import org.junit.Test; 
import org.junit.Before; 
import org.junit.After; 

/** 
* DialTestUtils Tester. 
* 
* @author <Authors name> 
* @since <pre>07/28/2017</pre> 
* @version 1.0 
*/ 
public class DialTestUtilsTest { 

@Before
public void before() throws Exception { 
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: TableList() 
* 
*/ 
@Test
public void testTableList() throws Exception {
    DialTestUtils.TableList();
} 

/** 
* 
* Method: DialTestAll() 
* 
*/ 
@Test
public void testDialTestAll() throws Exception {
    DialTestUtils.DialTestAll();
} 

/** 
* 
* Method: DialAttempt(String tableName) 
* 
*/ 
@Test
public void testDialAttemptTableName() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: DialAttempt(String tableName, int type) 
* 
*/ 
@Test
public void testDialAttemptForTableNameType() throws Exception {
    DialTestUtils.DialAttempt("okuc", 1);
} 

/** 
* 
* Method: DialTest(String tableName) 
* 
*/ 
@Test
public void testDialTest() throws Exception { 
//TODO: Test goes here... 
} 


} 
