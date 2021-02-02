package com.alibaba.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        args = new String[]{"/Users/wangmengyang/wmy-repositories/hadoop/hadoop/mapreduce/src/main/resources/inputflow/inputflow.txt",
                "/Users/wangmengyang/wmy-repositories/hadoop/hadoop/mapreduce/src/main/resources/outputflow"};
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        job.setJarByClass(FlowCountDriver.class);

        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        FileInputFormat.setInputPaths(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.waitForCompletion(true);
    }
}
