package com.alibaba.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSIoStream {
    // 上传文件到hdfs上
    @Test
    public void putFileToHDFS() throws IOException, InterruptedException, URISyntaxException {
        Configuration configuration = new Configuration();
        configuration.set("dfs.replication", "3");
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");
        FileInputStream fileInputStream = new FileInputStream(
                new File("/Users/wangmengyang/MyCode/hadoop/src/main/resources/elevenclass.txt"));
        FSDataOutputStream fsDataOutputStream = fileSystem.create(new Path("/elevenclass/elevenclass.txt"));
        IOUtils.copyBytes(fileInputStream,fsDataOutputStream,configuration);
        IOUtils.closeStream(fsDataOutputStream);
        IOUtils.closeStream(fileInputStream);
        fileSystem.close();
        System.out.println("over");
    }
    // 从hdfs上下载文件
    @Test
    public void getFileFromHDFS() throws Exception{
        Configuration configuration = new Configuration();
        // 1 获取对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");
        // 2 获取输入流
        FSDataInputStream fsDataInputStream = fileSystem.open(new Path("/input.txt"));
        // 3 获取输出流
        FileOutputStream fileOutputStream = new FileOutputStream(
                new File("/Users/wangmengyang/MyCode/hadoop/src/main/resources/input.txt"));
        // 4 流的对拷
        IOUtils.copyBytes(fsDataInputStream,fileOutputStream,configuration);
        // 5 关闭资源
        IOUtils.closeStream(fileOutputStream);
        IOUtils.closeStream(fsDataInputStream);
        fileSystem.close();
        System.out.println("over");
    }

    /**
     * 定位读取文件--读取块内容
     */
    // 下载第一块
    @Test
    public void readFileSeek1() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://nameservice01"), conf , "sa_cluster");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/user/sa_cluster/sa_cluster/hadoop-2.7.2.tar.gz"));

        // 3 获取输出流,读取到part1
        FileOutputStream fos = new FileOutputStream(
                new File("/Users/wangmengyang/MyCode/hadoop/src/main/resources/hadoop-2.7.2.tar.gz.part1"));

        // 4 流的对拷（只拷贝128m）
        byte[] buf = new byte[1024];
        for (int i = 0; i < 1024 * 128; i++) {
            fis.read(buf);
            fos.write(buf);
        }

        // 5 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
        System.out.println("over");
    }

    // 下载第二块
    @Test
    public void readFileSeek2() throws IOException, InterruptedException, URISyntaxException{

        // 1 获取对象
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI("hdfs://nameservice01"), conf , "sa_cluster");

        // 2 获取输入流
        FSDataInputStream fis = fs.open(new Path("/user/sa_cluster/sa_cluster/hadoop-2.7.2.tar.gz"));

        // 3 设置指定读取的起点
        fis.seek(1024*1024*128);

        // 4 获取输出流，将剩余数据读取的到part2
        FileOutputStream fos = new FileOutputStream(
                new File("/Users/wangmengyang/MyCode/hadoop/src/main/resources/hadoop-2.7.2.tar.gz.part2"));

        // 5 流的对拷
        IOUtils.copyBytes(fis, fos, conf);

        // 6 关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);
        fs.close();
        System.out.println("over");
    }
}
