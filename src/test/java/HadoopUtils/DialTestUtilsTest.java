package HadoopUtils; 

import HadoopUtils.utils.DialTestUtils;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After; 

/** 
* DialTestUtils Tester. 
* 
* @author <Authors name> 
* @since <pre>07/25/2017</pre> 
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
 * Method: DialTestAll()
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


} 
