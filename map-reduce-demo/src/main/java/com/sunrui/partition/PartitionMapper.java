package com.sunrui.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sunrui
 * @description 根据appkey把不同厂商的日志数据分别输出到不同的文件中
 * @date 2021/11/9
 */

public class PartitionMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {

    final PartitionBean bean = new PartitionBean();
    final Text k = new Text();
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] fields = value.toString().split("\t");
        String appKey = fields[2];

        bean.setId(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppkey(fields[2]);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThirdPartDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);

        k.set(appKey);
        context.write(k, bean);

    }
}
