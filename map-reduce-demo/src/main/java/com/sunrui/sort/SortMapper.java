package com.sunrui.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/9
 */
public class SortMapper extends Mapper<LongWritable, Text,SortBean, NullWritable> {

    final SortBean sortBean = new SortBean();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");

        sortBean.setDeviceId(fields[0]);
        sortBean.setSelfDrutation(Long.parseLong(fields[1]));
        sortBean.setThirdPartDuration(Long.parseLong(fields[2]));
        sortBean.setSumDuration(Long.parseLong(fields[4]));

        context.write(sortBean,NullWritable.get());
    }
}
