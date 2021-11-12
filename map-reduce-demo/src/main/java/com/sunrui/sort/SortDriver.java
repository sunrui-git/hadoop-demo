package com.sunrui.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/9
 */
public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "SortDriver");
        job.setJarByClass(SortDriver.class);

        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);

        job.setMapOutputKeyClass(SortBean.class);
        job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(SortBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定reduceTask的数量，默认是1个
        job.setNumReduceTasks(1);

        FileInputFormat.setInputPaths(job, new Path("d:\\speak\\out"));
        FileOutputFormat.setOutputPath(job, new Path("d:\\speak\\sortout"));

        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}
