package com.lagou.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/2
 */
public class HdfsClient {

    /**
     * 创建目录
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void testMkdir() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        // 配置在集群上运行
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://linux121:9000"),configuration,"root");
        // 2. 创建目录
        fileSystem.mkdirs(new Path("/test"));
        // 3. 关闭资源
        fileSystem.close();
    }

    /**
     * 本地文件上传hdfs
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void testCopyFromLocalFile() throws URISyntaxException, IOException, InterruptedException {
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "2");
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),configuration,"root");
        // 2 上传文件
        fs.copyFromLocalFile(new Path("D:/lagou.txt"),new Path("/lagou.txt"));
        // 3 关闭资源
        fs.close();
        System.out.println("end");
    }

    /**
     * 下载文件
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{
        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路径
        // Path dst 指将文件下载到的路径
        // boolean useRawLocalFileSystem 是否开启文件校验
        fs.copyToLocalFile(false, new Path("/lagou.txt"), new Path("D:/lagou_copy.txt"), true);
        fs.close();
    }

    /**
     * 删除文件
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void deleteFile() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),configuration,"root");
        fs.delete(new Path("/api_test/"),true);
        fs.close();
    }

    /**
     *  查看文件名称、权限、长度、块信息
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void listFiles() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),configuration,"root");
        RemoteIterator<LocatedFileStatus> fileList = fs.listFiles(new Path("/"), true);
        while (fileList.hasNext()){
            LocatedFileStatus status = fileList.next();
            System.out.println(status.getPath().getName());
            System.out.println(status.getLen());
            System.out.println(status.getPermission());
            System.out.println(status.getGroup());
            BlockLocation[] blockLocations = status.getBlockLocations();
            for(BlockLocation b : blockLocations){
                // 获取块存储的主机节点
                String[] hosts = b.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("-----------华丽的分割线----------");
        }
        fs.close();
    }

    /**
     * 文件夹判断
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void testListStatus() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),configuration,"root");
        // 2 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));
        for (FileStatus fileStatus : listStatus) {
            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }
        }
        // 3 关闭资源
        fs.close();
    }

    /**
     * I/O流上传文件
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void uploadFileToHdfs() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),configuration,"root");
        FileInputStream fis = new FileInputStream(new File("D:/lagou.txt"));
        FSDataOutputStream fos = fs.create(new Path("/lagou.txt"));
        IOUtils.copyBytes(fis,fos,configuration);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * I/O 流下载文件
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void downloadFileFormHdfs() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),configuration,"root");
        FSDataInputStream fis = new FSDataInputStream(fs.open(new Path("/lagou.txt")));
        FileOutputStream fos = new FileOutputStream(new File("D:/lagou.txt"));
        IOUtils.copyBytes(fis,fos,configuration);
        IOUtils.closeStream(fis);
        IOUtils.closeStream(fos);
        fs.close();
    }

    /**
     * 将HDFS上的lagou.txt的内容在控制台输出两次
     * @author sunrui
     * @date 2021/11/2
     * @param
     * @return void
     */
    @Test
    public void readFileSeek() throws URISyntaxException, IOException, InterruptedException {
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        FSDataInputStream fis = null;
        try{
            fis = fs.open(new Path("/lagou"));
            IOUtils.copyBytes(fis,System.out,4096,false);
            fis.seek(0);
            IOUtils.copyBytes(fis,System.out,4096,false);
        }finally {
            IOUtils.closeStream(fis);
            fs.close();
        }
    }
}
