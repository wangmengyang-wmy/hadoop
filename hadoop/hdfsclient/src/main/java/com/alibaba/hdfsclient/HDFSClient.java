package com.alibaba.hdfsclient;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

public class HDFSClient {
    public static void main(String[] args) throws IOException, URISyntaxException, InterruptedException {
        // 偶尔出现读取不到环境变量的情况时，使用下面的方式
//        System.setProperty("hadoop.home.dir", "/Users/wangmengyang/Public/hadoop-2.7.2");

        Configuration configuration = new Configuration();
//        configuration.set("fs.defaultFS","hdfs://bigdata101:9000");
        // 获取hdfs客户端对象
//        FileSystem fileSystem = FileSystem.get(configuration);
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");

        // 在hdfs上创建目录
        Path path = new Path("/user/bigdata2");
        fileSystem.mkdirs(path);
        // 关闭资源
        fileSystem.close();

        System.out.println("over");
    }
    // 文件上传
    @Test
    public void testCopyFromLocalFile() throws Exception {
        Configuration configuration = new Configuration();
        // 通过代码的形式设置副本数：
        // 优先级：代码>resources配置文件>hdfs-site.xml>默认配置
//        configuration.set("dfs.replication", "2");
        // 获取hdfs客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");
        // 创建文件夹
//        fileSystem.mkdirs(new Path("/elevenclass"));
        // 上传文件
        fileSystem.copyFromLocalFile(
                new Path("/Users/wangmengyang/MyCode/hadoop/src/main/resources/elevenclass.txt"),
                new Path("/elevenclass")); // 上传的时候可以指定文件名称，不指定的时候采用的是上传的文件名
        fileSystem.close();
        System.out.println("over");
    }
    // 文件下载
    @Test
    public void testCopyToLocalFile() throws Exception{
        Configuration configuration = new Configuration();
        // 获取hdfs客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");
        // 文件下载：默认下载下来的是和源文件名相同，也可以指定下载后的文件名
//        fileSystem.copyToLocalFile(
//                new Path("/elevenclass/elevenclass.txt"),
//                new Path("/Users/wangmengyang/MyCode/hadoop/src/main/resources/down/tezhanban.txt"));

        // delSrc：删除hdfs上的文件，默认是false
        // useRawLocalFileSystem：采用本地文件系统，不会产生crc校验文件
        fileSystem.copyToLocalFile(false,
                new Path("/elevenclass/elevenclass.txt"),
                new Path("/Users/wangmengyang/MyCode/hadoop/src/main/resources/down/elevenclass.txt"),
                true);
        // 关闭资源
        fileSystem.close();
        System.out.println("over");
    }
    // 文件删除
    @Test
    public void testDelete() throws Exception{
        Configuration configuration = new Configuration();
        // 获取hdfs客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");
        // true，是否递归删除，如果是文件设置为true也无所谓，如果是目录必须设置为true
        fileSystem.delete(new Path("/user"), true);
        fileSystem.close();
        System.out.println("over");
    }

    // 文件名称修改
    @Test
    public void testRename() throws Exception{
        Configuration configuration = new Configuration();
        // 获取hdfs客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");
        fileSystem.rename(new Path("/elevenclass/elevenclass.txt"),new Path("/elevenclass/tezhanban.txt"));
        fileSystem.close();
        System.out.println("over");
    }

    // 文件详情查看
    @Test
    public void testListFiles() throws Exception{
        Configuration configuration = new Configuration();
        // 获取hdfs客户端对象
        FileSystem fileSystem = FileSystem.get(new URI("hdfs://bigdata101:9000"), configuration, "bigdata");
        // recursive：是否递归
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fileSystem.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()){
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println("名称："+next.getPath().getName());
            System.out.println("权限："+next.getPermission());
            System.out.println("长度："+next.getLen());
            BlockLocation[] blockLocations = next.getBlockLocations();
            for (BlockLocation blockLocation:blockLocations) {
                String[] hosts = blockLocation.getHosts();
                for (String host:hosts) {
                    System.out.println("机器："+host);
                }
            }
            System.out.println("--------分割线--------");
        }
        fileSystem.close();
    }

    // 判断是文件还是文件夹
    @Test
    public void testListStatus() throws Exception{
        ListStatus(new Path("/user/sa_cluster"));
        fileSystem.close();
    }

    private static Configuration configuration = new Configuration();
    private static FileSystem fileSystem = null;

    static {
        try {
            // 获取hdfs客户端对象
            fileSystem = FileSystem.get(new URI("hdfs://nameservice01"), configuration, "sa_cluster");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void ListStatus(Path path) throws Exception{
        if (fileSystem == null) {
            System.out.println("fileSystem is null");
            return;
        }
        FileStatus[] fileStatuses = fileSystem.listStatus(path);
        if (fileStatuses != null&&fileStatuses.length!=0) {
            for (FileStatus fileStatus:fileStatuses) {
                if (fileStatus.isDirectory()) {// 如果想要遍历全部文件需要加一个递归
                    System.out.println(fileStatus.getPath().toString()+" is a Directory");
                    Path path1 = fileStatus.getPath();
                    ListStatus(path1);
                }else {
                    System.out.println(fileStatus.getPath().toString()+" is a File");
                }
            }
        }
    }
}
