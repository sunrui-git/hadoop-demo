package com.sunrui.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author sunrui
 * @description 数字排序
 * @date 2021/11/11
 */
public class NumSortReducer extends Reducer<IntWritable, IntWritable,IntWritable,IntWritable> {

    // 排序的序号
    int i = 0;
    final IntWritable index = new IntWritable();
    @Override
    protected void reduce(IntWritable key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

        for(IntWritable value : values){
            i = value.get() + i;
            index.set(i);
            context.write(index, key);
        }
    }
}
