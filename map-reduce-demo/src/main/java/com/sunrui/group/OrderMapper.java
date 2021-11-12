package com.sunrui.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/10
 */
public class OrderMapper extends Mapper<LongWritable, Text,OrderBean, NullWritable> {

    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        bean.setOrderId(fields[0]);
        bean.setPrice(Double.parseDouble(fields[2]));

        context.write(bean,NullWritable.get());
    }
}
