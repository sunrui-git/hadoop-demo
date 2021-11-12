package com.sunrui.comment.step2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.DefaultCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/11
 */
public class MergeDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "MergeDriver");
        job.setJarByClass(MergeDriver.class);
        job.setMapperClass(MergeMapper.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);

        //设置使用自定义InputFormat读取数据
        job.setInputFormatClass(MergeInputFormat.class);
        FileInputFormat.setInputPaths(job, new Path("E:\\lagou\\data\\mr\\merge-out")); //指定读取数据的原始路径
        //指定输出使用的outputformat
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        //尽可能降低数据的量，减少磁盘空间的占用，网络间通信时数据量小可以节省时间
        //针对Sequencefile的压缩
        SequenceFileOutputFormat.setOutputCompressorClass(job, DefaultCodec.class);
        //压缩类型：record压缩
        SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.RECORD);
        // SequenceFileOutputFormat.setOutputCompressionType(job, SequenceFile.CompressionType.BLOCK);

        FileOutputFormat.setOutputPath(job, new Path("E:\\lagou\\data\\mr\\out")); //指定结果数据输出路径

        final boolean flag = job.waitForCompletion(true);
        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);
    }
}
