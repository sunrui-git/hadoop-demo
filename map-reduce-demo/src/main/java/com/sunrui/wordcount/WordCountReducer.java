package com.sunrui.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/8
 */
//继承的Reducer类有四个泛型参数,2对kv
//第一对kv:类型要与Mapper输出类型一致：Text, IntWritable
//第二队kv:自己设计决定输出的结果数据是什么类型：Text, IntWritable
public class WordCountReducer extends Reducer<Text, IntWritable,Text,IntWritable> {
    //1 重写reduce方法

    //Text key:map方法输出的key,本案例中就是单词,
    // Iterable<IntWritable> values：一组key相同的kv的value组成的集合
    IntWritable total = new IntWritable();
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        //2 遍历key对应的values，然后累加结果
        for(IntWritable value : values){
            int i = value.get();
            sum += i;
        }
        // 3 直接输出当前key对应的sum值，结果就是单词出现的总次数
        total.set(sum);
        context.write(key,total);
    }
}
