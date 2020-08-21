package com.donaldy;

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
 * @author donald
 * @date 2020/08/02
 */
public class HdfsClientDemo {

    @Test
    public void testMkdirs() throws URISyntaxException, IOException, InterruptedException {

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        // 配置在集群上运行
        // configuration.set("fs.defaultFS", "hdfs://linux121:9000");
        // FileSystem fs = FileSystem.get(configuration);
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");
        // 2 创建目目录
        fs.mkdirs(new Path("/test2"));

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testCopyFromLocalToHdfs() throws URISyntaxException, IOException, InterruptedException {

        Configuration configuration = new Configuration();

        // 设置副本数
        // configuration.set("dfs.replication", "2");

        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");

        fs.copyFromLocalFile(new Path("/home/donald/donald.txt"), new Path("/donald.txt"));

        // 上传文件到 hdfs，默认副本是3个

        fs.close();
    }

    @Test
    public void testCopyToLocalFile() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"), configuration, "root");

        // 2 执行下载操作
        // boolean delSrc 指是否将原文件删除
        // Path src 指要下载的文件路路径
        // Path dst 指将文件下载到的路路径
        // boolean useRawLocalFileSystem 是否开启文文件校验
        fs.copyToLocalFile(false, new Path("/donald.txt"),
                new Path("/home/donald/donald_copy.txt"), true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testDelete() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");

        // 2 执行删除, 是否递归删除
        fs.delete(new Path("/test"), true);

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testListFiles() throws IOException, InterruptedException, URISyntaxException{

        // 1获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");

        // 2 获取文件详情
        RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"),
                true);

        while(listFiles.hasNext()){
            LocatedFileStatus status = listFiles.next();
            // 输出详情
            // 文件名称
            System.out.println(status.getPath().getName());
            // 长度
            System.out.println(status.getLen());// 权限
            System.out.println(status.getPermission());
            // 分组
            System.out.println(status.getGroup());
            // 获取存储的块信息
            BlockLocation[] blockLocations = status.getBlockLocations();
            for (BlockLocation blockLocation : blockLocations) {
            // 获取块存储的主机节点
                String[] hosts = blockLocation.getHosts();
                for (String host : hosts) {
                    System.out.println(host);
                }
            }
            System.out.println("----------- line ----------");
        }

        // 3 关闭资源
        fs.close();
    }

    @Test
    public void testListStatus() throws IOException, InterruptedException, URISyntaxException{

        // 1. 获取文件配置信息
        Configuration configuration = new Configuration();

        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");

        // 2. 判断是文件还是文件夹
        FileStatus[] listStatus = fs.listStatus(new Path("/"));

        for (FileStatus fileStatus : listStatus) {

            // 如果是文件
            if (fileStatus.isFile()) {
                System.out.println("f:"+fileStatus.getPath().getName());
            }else {
                System.out.println("d:"+fileStatus.getPath().getName());
            }}

        // 3. 关闭资源
        fs.close();
    }

    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        // 1. 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");
        // 2. 创建输入流
        FileInputStream fis = new FileInputStream(new File("/home/donald/donald.txt"));

        // 3. 获取输出流
        FSDataOutputStream fos = fs.create(new Path("/donald_io.txt"));

        // 4. 流对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5. 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

        fs.close();
    }

    @Test
    public void getFileFromHDFS() throws IOException, InterruptedException, URISyntaxException{
        // 1. 获取文文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");
        // 2. 获取输入入流
        FSDataInputStream fis = fs.open(new Path("/donald_io.txt"));

        // 3. 获取输出流
        FileOutputStream fos = new FileOutputStream(new File("/home/donald/donald_io_copy.txt"));

        // 4. 流的对拷
        IOUtils.copyBytes(fis, fos, configuration);

        // 5. 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
    }

    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{
        // 1. 获取文件系统
        Configuration configuration = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://linux121:9000"),
                configuration, "root");

        // 2. 打开输入流,读取数据输出到控制台
        FSDataInputStream in = null;

        try{
            in= fs.open(new Path("/donald_io.txt"));

            IOUtils.copyBytes(in, System.out, 4096, false);
            in.seek(0);
            //从头再次读取
            IOUtils.copyBytes(in, System.out, 4096, false);

        } finally {
            IOUtils.closeStream(in);
        }
    }
}
