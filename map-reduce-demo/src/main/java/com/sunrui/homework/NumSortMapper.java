package com.sunrui.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sunrui
 * @description 数字排序
 * @date 2021/11/11
 */
public class NumSortMapper extends Mapper<LongWritable, Text, IntWritable, IntWritable> {

    final IntWritable one = new IntWritable(1);

    IntWritable num = new IntWritable();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 一行一个数字，可直接写入
        num.set(Integer.parseInt(value.toString()));
        context.write(num,one);
    }
}
