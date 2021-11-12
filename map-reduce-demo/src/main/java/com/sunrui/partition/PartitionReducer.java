package com.sunrui.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/9
 */
public class PartitionReducer extends Reducer<Text,PartitionBean,Text,PartitionBean> {

    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context) throws IOException, InterruptedException {

        for(PartitionBean value : values){
            context.write(key,value);
        }
    }
}
