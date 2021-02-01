package com.alibaba.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {

    IntWritable v = new IntWritable();

    /**
     *
     * @param key map阶段输出的键
     * @param values 每一个键对应的value的值的迭代器
     * @param context 上下文
     */
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        int sum = 0;

        for (IntWritable value:values) {
            sum += value.get();
        }
        v.set(sum);
        context.write(key, v);
    }
}
