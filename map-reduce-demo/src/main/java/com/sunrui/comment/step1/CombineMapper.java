package com.sunrui.comment.step1;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/11
 */
public class CombineMapper extends Mapper<LongWritable, Text, NullWritable,Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        context.write(NullWritable.get(),value);
    }
}
