package com.alibaba.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {


    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> flowBeans, Context context) throws IOException, InterruptedException {
        long upFlow = 0;
        long downFlow = 0;

        for (FlowBean flowBean:flowBeans) {
            upFlow += flowBean.getUpFlow();
            downFlow += flowBean.getDownFlow();
        }
        flowBean.set(upFlow,downFlow);
        context.write(key, flowBean);
    }
}
