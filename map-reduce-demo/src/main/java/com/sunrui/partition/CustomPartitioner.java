package com.sunrui.partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author sunrui
 * @description 自定义分区 Partitioner分区器的泛型是map输出的kv类型
 * @date 2021/11/9
 */
public class CustomPartitioner extends Partitioner<Text,PartitionBean> {

    @Override
    public int getPartition(Text key, PartitionBean value, int numPartition) {
        int partition = 0;
        if (key.toString().equals("kar")) {
            //只需要保证满足此if条件的数据获得同个分区编号集合
            partition = 0;
        } else if (key.toString().equals("pandora")) {
            partition = 1;
        } else {
            partition = 2;
        }
        return partition;
    }
}
