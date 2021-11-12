package com.sunrui.speak;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/8
 */
public class SpeakReducer extends Reducer<Text,SpeakBean,Text, SpeakBean> {
    //reduce方法的key:map输出的某一个key
    //reduce方法的value:map输出的kv对中相同key的value组成的一个集合
    //reduce 逻辑：遍历迭代器累加时长即可


    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context) throws IOException, InterruptedException {
        Long self_duration = 0L;
        Long third_part_duration = 0L;

        for (SpeakBean bean : values) {
            self_duration += bean.getSelfDuration();
            third_part_duration += bean.getThirdPartDuration();
        }
        SpeakBean speakBean = new SpeakBean(self_duration,third_part_duration,key.toString());
        context.write(key,speakBean);
    }
}
