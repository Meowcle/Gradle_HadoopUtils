package HadoopUtils;

/**
 * Created by Meowcle~ on 2017/7/14.
 */

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

public class HdfsUtils {

    public static String uri = "hdfs://localhost:9000";

    // hadoop fs的配置文件
    static Configuration conf = new Configuration(true);
    static {
        // 指定hadoop fs的地址
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        conf.set("fs.defaultFS", uri);
    }

    /**
     *
     * 判断路径是否存在
     *
     * @path 路径
     */
    public static boolean isExists(String path) throws IOException {
        if (StringUtils.isBlank(path)){
            System.err.println("Invalid path!");
            System.exit(0);
        }
        FileSystem fs = FileSystem.get(conf);
        return fs.exists(new Path(path));
    }

    /**
     *
     * 创建文件
     *
     * @filePath 路径
     *
     * @contents 内容
     */
    public static boolean createFile(String filePath, byte[] contents)
            throws IOException {
        if (StringUtils.isBlank(filePath)){
            System.err.println("Invalid path!");
            return false;
        }
        if (isExists(filePath)){
            System.err.println("File already exists!");
            return false;
        }
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        FSDataOutputStream outputStream = fs.create(path);
        outputStream.write(contents);
        outputStream.close();
        fs.close();
        return true;
    }

    /**
     *
     * 创建文件
     *
     * @filePath 路径
     *
     * @fileContent 内容
     */
    public static boolean createFile(String filePath, String fileContent)
            throws IOException {
        if (StringUtils.isBlank(filePath)){
            System.err.println("Invalid path!");
            return false;
        }
        createFile(filePath, fileContent.getBytes());
        return true;
    }

    /**
     *
     * 上传文件到HDFS
     *
     * @localFilePath 本地文件路径
     *
     * @remoteFilePath 远端文件路径
     */
    public static boolean copyFromLocalFile(String localFilePath,
                                         String remoteFilePath) throws IOException {
        if (StringUtils.isBlank(remoteFilePath)){
            System.err.println("Invalid remote path!");
            return false;
        }
        if (StringUtils.isBlank(localFilePath)){
            System.err.println("Invalid local path!");
            return false;
        }
        HdfsUtils.deleteFile(remoteFilePath);
        FileSystem fs = FileSystem.get(conf);
        Path localPath = new Path(localFilePath);
        Path remotePath = new Path(remoteFilePath);
        fs.copyFromLocalFile(false, true, localPath, remotePath);
        System.out.println("File uploaded from " + localPath + " to " + remotePath);
        fs.close();
        return true;
    }

    /**
     *
     * 下载HDFS上的文件
     *
     * @remotePath 远端路径
     *
     * @localPath 本地路径
     */
    public static boolean download(String remotePath, String localPath) throws IOException {
        if (StringUtils.isBlank(remotePath)){
            System.err.println("Invalid remote path!");
            return false;
        }
        if (StringUtils.isBlank(localPath)){
            System.err.println("Invalid local path!");
            return false;
        }
        Path localFilePath = new Path(localPath);
        Path remoteFilePath = new Path(remotePath);
        FileSystem fs = FileSystem.get(conf);
        fs.copyToLocalFile(remoteFilePath, localFilePath);
        System.out.println("File download from " + remotePath + " to " + localPath);
        fs.close();
        return true;
    }

    /**
     *
     * 文件重命名
     *
     * @oldFileName 原文件名（路径）
     *
     * @newFileName 新文件名（路径）
     */
    public static boolean renameFile(String oldFileName, String newFileName)
            throws IOException {
        if (StringUtils.isBlank(oldFileName)){
            System.err.println("Invalid old path!");
            return false;
        }
        if (StringUtils.isBlank(newFileName)){
            System.err.println("Invalid new path!");
            return false;
        }
        if (HdfsUtils.isExists(newFileName)){
            System.err.println("Filename already exists!");
            return false;
        }
        FileSystem fs = FileSystem.get(conf);
        Path oldPath = new Path(oldFileName);
        Path newPath = new Path(newFileName);
        boolean result = fs.rename(oldPath, newPath);
        fs.close();
        return result;
    }

    /**
     *
     * 创建目录
     *
     * @dirName 目录名
     */
    public static boolean createDirectory(String dirName) throws IOException {
        boolean result = false;
        if (StringUtils.isBlank(dirName)){
            System.err.println("Invalid path!");
            System.exit(0);
            return result;
        }
        FileSystem fs = FileSystem.get(conf);
        Path dir = new Path(dirName);
        if (!fs.exists(dir)) {
            result = fs.mkdirs(dir);
        }
        fs.close();
        return result;
    }

    /**
     *
     * 删除目录或文件
     *
     * @remoteFilePath 远端文件路径
     *
     * @recursive 级联
     */
    public static boolean deleteFile(String remoteFilePath, boolean recursive)
            throws IOException {
        if (StringUtils.isBlank(remoteFilePath)){
            System.err.println("Invalid path!");
            return false;
        }
        FileSystem fs = FileSystem.get(conf);
        boolean result = fs.delete(new Path(remoteFilePath), recursive);
        fs.close();
        return result;
    }

    /**
     *
     * 删除目录或文件(如果有子目录,则级联删除)
     *
     * @remoteFilePath 远端文件路径
     */
    public static boolean deleteFile(String remoteFilePath) throws IOException {
        if (StringUtils.isBlank(remoteFilePath)){
            System.err.println("Invalid path!");
            return false;
        }
        return deleteFile(remoteFilePath, true);
    }

    public static List<String> listAll(String dir) throws IOException {
        if (StringUtils.isBlank(dir)) {
            return new ArrayList<String>();
        }
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] stats = fs.listStatus(new Path(uri + dir));
        List<String> names = new ArrayList<String>();
        for (int i = 0; i < stats.length; ++i) {
            if (stats[i].isFile()) {
                // regular file
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isDirectory()) {
                // dir
                names.add(stats[i].getPath().toString());
            } else if (stats[i].isSymlink()) {
                // is s symlink in linux
                names.add(stats[i].getPath().toString());
            }
        }
        fs.close();
        return names;
    }

    /**
     *
     * 列出指定路径下的所有文件(不包含目录)
     *
     * @basePath 路径
     *
     * @recursive
     */
    public static RemoteIterator<LocatedFileStatus> listFiles(String basePath,
                                                              boolean recursive) throws IOException {
        if (StringUtils.isBlank(basePath)){
            System.err.println("Invalid path!");
            System.exit(0);
        }
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> fileStatusRemoteIterator = fs
                .listFiles(new Path(basePath), recursive);

        return fileStatusRemoteIterator;
    }

    /**
     *
     * 列出指定路径下的文件（非递归）
     *
     * @param basePath
     * @return
     * @throws IOException
     */
    public static RemoteIterator<LocatedFileStatus> listFiles(String basePath)
            throws IOException {
        if (StringUtils.isBlank(basePath)){
            System.err.println("Invalid path!");
            System.exit(0);
        }
        FileSystem fs = FileSystem.get(conf);
        RemoteIterator<LocatedFileStatus> remoteIterator = fs.listFiles(
                new Path(basePath), false);
        fs.close();
        return remoteIterator;
    }

    /**
     *
     * 列出指定目录下的文件/子目录信息
     *
     * @dirPath 路径
     */
    public static FileStatus[] listStatus(String dirPath) throws IOException {
        if (StringUtils.isBlank(dirPath)){
            System.err.println("Invalid path!");
            System.exit(0);
        }
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileStatuses = fs.listStatus(new Path(dirPath));
        fs.close();
        return fileStatuses;
    }

    /**
     *
     * 读取文件内容
     *
     * @filePath 文件路径
     */
    public static byte[] readFile(String filePath) throws IOException {
        if (StringUtils.isBlank(filePath)){
            System.err.println("Invalid path!");
            System.exit(0);
        }
        byte[] fileContent = null;
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        if (fs.exists(path)) {
            InputStream inputStream = null;
            ByteArrayOutputStream outputStream = null;
            try {
                inputStream = fs.open(path);
                outputStream = new ByteArrayOutputStream(
                        inputStream.available());
                IOUtils.copyBytes(inputStream, outputStream, conf);
                fileContent = outputStream.toByteArray();
            } finally {
                IOUtils.closeStream(inputStream);
                IOUtils.closeStream(outputStream);
                fs.close();
            }
        }
        return fileContent;
    }

    /**
     *
     * 读取文件内容，返回输入流
     *
     * @filePath 文件路径
     */
    public static InputStream readFileStream(String filePath) throws IOException {
        if (StringUtils.isBlank(filePath)){
            System.err.println("Invalid path!");
            System.exit(0);
        }
        FileSystem fs = FileSystem.get(conf);
        Path path = new Path(filePath);
        if (fs.exists(path)) {
            InputStream inputStream = fs.open(path);
            return inputStream;
        }
        return null;
    }

    /**
     *
     * 追加文件内容
     *
     * @filePath 文件路径
     *
     * @content 追加内容
     */

    public static boolean append(String filePath, String content) throws Exception {
        if (StringUtils.isBlank(filePath)){
            System.err.println("Invalid path!");
            System.exit(0);
        }
        if(StringUtils.isEmpty(content)){
            return true;
        }
        conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
        conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
        FileSystem fs = FileSystem.get(conf);
        // check if the file exists
        Path path = new Path(filePath);
        if (fs.exists(path)) {
            try {
                InputStream in = new ByteArrayInputStream(content.getBytes());
                OutputStream out = fs.append(new Path(filePath));
                IOUtils.copyBytes(in, out, 4096, true);
                out.close();
                in.close();
                fs.close();
            } catch (Exception ex) {
                fs.close();
                throw ex;
            }
        } else {
            HdfsUtils.createFile(filePath, content);
        }
        return true;
    }
}
