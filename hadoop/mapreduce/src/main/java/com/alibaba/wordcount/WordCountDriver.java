package com.alibaba.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args= new String[]{"/Users/wangmengyang/wmy-repositories/hadoop/hadoop/mapreduce/src/main/resources/input/input.txt",
                "/Users/wangmengyang/wmy-repositories/hadoop/hadoop/mapreduce/src/main/resources/output"};
        Configuration configuration = new Configuration();

        // 1、获取job对象
        Job job = Job.getInstance(configuration);
        // 2、设置Driver的jar存储位置
        job.setJarByClass(WordCountDriver.class);
        // 3、关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        // 4、设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 5、设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        // 6、设置输入路径和输出路径
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));
        // 7、提交job，参数为true时会打印job信息
        job.waitForCompletion(true);
    }
}
