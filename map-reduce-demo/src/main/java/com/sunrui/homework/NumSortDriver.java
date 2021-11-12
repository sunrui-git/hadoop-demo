package com.sunrui.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author sunrui
 * @description 数字排序
 * @date 2021/11/11
 */
public class NumSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取配置文件对象，获取job对象实例
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"NumSortDriver");
        // 指定程序jar的本地路径
        job.setJarByClass(NumSortDriver.class);
        // 指定Mapper/Reducer类
        job.setMapperClass(NumSortMapper.class);
        job.setReducerClass(NumSortReducer.class);
        // 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(IntWritable.class);
        // 指定最终输出的kv数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);
        // 指定reduceTask的数量
        job.setNumReduceTasks(1);
        // 指定job处理的原始数据路径
        FileInputFormat.setInputPaths(job,new Path("map-reduce-demo/src/main/java/com/sunrui/homework/homework"));
        // 指定job输出结果路径
        FileOutputFormat.setOutputPath(job,new Path("map-reduce-demo/src/main/java/com/sunrui/homework/homework/out"));

        boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}
