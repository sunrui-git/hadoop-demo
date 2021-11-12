package com.sunrui.speak;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/8
 */
//四个参数：分为两对kv
//第一对kv:map输入参数的kv类型；k-->一行文本偏移量，v-->一行文本内容
//第二对kv：map输出参数kv类型;k-->map输出的key类型，v:map输出的value类型
public class SpeakMapper extends Mapper<LongWritable, Text,Text,SpeakBean> {
    /*
    1 转换接收到的text数据为String
    2 按照制表符进行切分；得到自有内容时长，第三方内容时长，设备id,封装为SpeakBean
    3 直接输出：k-->设备id,value-->speakbean
     */
    Text deviceIdText = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        //自有内容时长
        String selfDuration = fields[fields.length - 3];
        //第三方内容时长
        String thirdPartDuration = fields[fields.length - 2];
        //设备id
        String deviceId = fields[1];
        SpeakBean speakBean = new SpeakBean(Long.parseLong(selfDuration), Long.parseLong(thirdPartDuration), deviceId);
        deviceIdText.set(deviceId);
        context.write(deviceIdText,speakBean);
    }
}
