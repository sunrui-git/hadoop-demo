package com.sunrui.comment.step3;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/12
 */
public class CommentMapper extends Mapper<Text, BytesWritable,CommentBean, NullWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        //key就是文件名 value:一个文件的完整内容
        String str = new String(value.getBytes());
        String[] lines = str.split("\n");
        for(String line : lines){
            CommentBean bean = parseStrToCommentBean(line);
            if(bean != null){
                context.write(bean,NullWritable.get());
            }
        }
    }
    //切分字符串封装成commentBean对象
    private CommentBean parseStrToCommentBean(String line) {
        if (StringUtils.isNotBlank(line)) {
            //每一行进行切分
            String[] fields = line.split("\t");
            if (fields.length >= 9) {
                return new CommentBean(fields[0], fields[1], fields[2], Integer.parseInt(fields[3]), fields[4], fields[5], fields[6], Integer.parseInt(fields[7]),
                        fields[8]);
            }
        }
        return null;
    }
}
