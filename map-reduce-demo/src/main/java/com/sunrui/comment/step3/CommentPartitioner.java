package com.sunrui.comment.step3;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author sunrui
 * @description
 * @date 2021/11/12
 */
public class CommentPartitioner extends Partitioner<CommentBean, NullWritable> {
    @Override
    public int getPartition(CommentBean commentBean, NullWritable nullWritable, int i) {
        return commentBean.getCommentStatus();
    }
}
