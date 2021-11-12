package com.sunrui.partition;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/9
 */
public class PartitionDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf,"PartitionDriver");

        job.setJarByClass(PartitionDriver.class);
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PartitionBean.class);

        // 4 设置使用自定义分区器
        job.setPartitionerClass(CustomPartitioner.class);
        //5 指定reducetask的数量与分区数量保持一致,分区数量是3
        job.setNumReduceTasks(3); //reducetask不设置默认是1个

        // 6 指定输入和输出数据路径
        FileInputFormat.setInputPaths(job, new Path("d:/speak.data"));
        FileOutputFormat.setOutputPath(job, new Path("d:/parition/out"));
        // 7 提交任务
        final boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}
