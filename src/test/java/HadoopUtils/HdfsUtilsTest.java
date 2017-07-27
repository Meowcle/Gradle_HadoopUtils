package HadoopUtils; 

import HadoopUtils.utils.HdfsUtils;
import org.junit.Test;
import org.junit.Before; 
import org.junit.After;

import static org.junit.Assert.assertEquals;

/** 
* HdfsUtils Tester.
* 
* @author Meowcle~
* @since <pre>07/14/2017</pre> 
* @version 1.0 
*/ 
public class HdfsUtilsTest {

    public static String uri = "hdfs://localhost:9000";
    public String dir1 = "/Meowcle/dir1";
    public String dir2 = "/Meowcle/dir2";

@Before
public void before() throws Exception {
} 

@After
public void after() throws Exception { 
} 

/** 
* 
* Method: exits(String path) 
* 
*/ 
@Test
public void testExits() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: createFile(String filePath, byte[] contents) 
* 
*/ 
@Test
public void testCreateFileForFilePathContents() throws Exception {
    try{
        String newFile = dir2 + "/file1.txt";
        String content = "Tis file1.txt";
        assertEquals(true, HdfsUtils.createFile(newFile, content));
        String result = new String(HdfsUtils.readFile(newFile));
        System.out.println(result);
        assertEquals(content, result);
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: createFile(String filePath, String fileContent) 
* 
*/ 
@Test
public void testCreateFileForFilePathFileContent() throws Exception {
    try{
        String newFile = dir1 + "/file2.txt";
        String content = "Tis file2.txt";
        assertEquals(true, HdfsUtils.createFile(newFile, content));
        String result = new String(HdfsUtils.readFile(newFile));
        assertEquals(content, result);
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: copyFromLocalFile(String localFilePath, String remoteFilePath) 
* 
*/ 
@Test
public void testCopyFromLocalFile() throws Exception {
    String localFilePath = "D:/uploadtest.txt";
    String remoteFilePath = dir1 + "/FileUploadAndDownloadTest.txt";
    try{
        assertEquals(true, HdfsUtils.copyFromLocalFile(localFilePath, remoteFilePath));
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: download(String remotePath, String localPath) 
* 
*/ 
@Test
public void testDownload() throws Exception {
    String localFilePath = "D:/downloadtest.txt";
    String remoteFilePath = dir1 + "/FileUploadAndDownloadTest.txt";
    try{
        assertEquals(true, HdfsUtils.download(remoteFilePath, localFilePath));
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: renameFile(String oldFileName, String newFileName) 
* 
*/ 
@Test
public void testRenameFile() throws Exception {
    String oldFileName = dir1 + "file1.txt";
    String newFIleName = dir1 + "RenamedFile.txt";
    try{
        assertEquals(true, HdfsUtils.renameFile(oldFileName, newFIleName));
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: createDirectory(String dirName) 
* 
*/ 
@Test
public void testCreateDirectory() throws Exception {
    String dirName = dir1;
    try{
        assertEquals(true, HdfsUtils.createDirectory(dirName));
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: deleteFile(String remoteFilePath, boolean recursive) 
* 
*/ 
@Test
public void testDeleteFileForRemoteFilePathRecursive() throws Exception {
//TODO: Test goes here...
} 

/** 
* 
* Method: deleteFile(String remoteFilePath) 
* 
*/ 
@Test
public void testDeleteFileRemoteFilePath() throws Exception {
    String dirName = dir2;
    try{
        assertEquals(true, HdfsUtils.deleteFile(dirName));
    } catch(Exception ex){
        ex.printStackTrace();
        assertEquals(true, false);
    }
} 

/** 
* 
* Method: listFiles(String basePath, boolean recursive) 
* 
*/ 
@Test
public void testListFilesForBasePathRecursive() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: listFiles(String basePath) 
* 
*/ 
@Test
public void testListFilesBasePath() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: listStatus(String dirPath) 
* 
*/ 
@Test
public void testListStatus() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: readFile(String filePath) 
* 
*/ 
@Test
public void testReadFile() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: readFileStream(String filePath) 
* 
*/ 
@Test
public void testReadFileStream() throws Exception { 
//TODO: Test goes here... 
} 

/** 
* 
* Method: append(String filePath, String content) 
* 
*/ 
@Test
public void testAppend() throws Exception { 
//TODO: Test goes here... 
} 


} 
