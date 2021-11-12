package com.sunrui.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @author sunrui
 * @description
 * @date 2021/11/9
 */
public class SortBean implements WritableComparable<SortBean> {
    private Long selfDrutation;//自有内容播放时长
    private Long thirdPartDuration;//第三方内容播放时长
    private String deviceId;//设备id
    private Long sumDuration;//总时长

    public SortBean() {
    }

    public SortBean(Long selfDrutation, Long thirdPartDuration, String deviceId, Long sumDuration) {
        this.selfDrutation = selfDrutation;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDuration = sumDuration;
    }

    //返回值三种：0：相等 1：小于 -1：大于
    public int compareTo(SortBean bean) {
        System.out.println("compareTo 方法执行了。。。");
        //指定按照bean对象的总时长字段的值进行比较
        if (this.sumDuration > bean.sumDuration) {
            return -1;
        } else if (this.sumDuration < bean.sumDuration) {
            return 1;
        } else {
            return 0; //加入第二个判断条件，二次排序
        }
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeLong(selfDrutation);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeUTF(deviceId);
        dataOutput.writeLong(sumDuration);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.selfDrutation = dataInput.readLong();
        this.thirdPartDuration = dataInput.readLong();
        this.deviceId = dataInput.readUTF();
        this.sumDuration = dataInput.readLong();
    }

    @Override
    public boolean equals(Object o) {
        System.out.println("equals方法执行了。。。");
        return super.equals(o);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSelfDrutation(), getThirdPartDuration(), getDeviceId(), getSumDuration());
    }

    public Long getSelfDrutation() {
        return selfDrutation;
    }

    public void setSelfDrutation(Long selfDrutation) {
        this.selfDrutation = selfDrutation;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public Long getSumDuration() {
        return sumDuration;
    }

    public void setSumDuration(Long sumDuration) {
        this.sumDuration = sumDuration;
    }
}
