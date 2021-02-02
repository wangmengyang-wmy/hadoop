package com.alibaba.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text k = new Text();
    FlowBean flowBean = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split("\t");
        // 手机号
        k.set(words[1]);
        long upFlow = Long.parseLong(words[words.length - 3]);
        long downFlow = Long.parseLong(words[words.length - 2]);
        flowBean.set(upFlow,downFlow);
        context.write(k,flowBean);
    }
}
