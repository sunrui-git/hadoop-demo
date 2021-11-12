package com.sunrui.partition;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author sunrui
 * @description
 * @date 2021/11/9
 */
public class PartitionBean implements Writable {
    //定义属性
    private String id;//日志id
    private String deviceId;//设备id
    private String appkey;//appkey厂商id
    private String ip;//ip地址
    private Long selfDuration;//自有内容播放时长
    private Long thirdPartDuration;//第三方内容时长
    private String status;//状态码
    public PartitionBean() {

    }

    public PartitionBean(String id, String deviceId, String appkey, String ip, Long selfDuration, Long thirdPartDuration, String status) {
        this.id = id;
        this.deviceId = deviceId;
        this.appkey = appkey;
        this.ip = ip;
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.status = status;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(id);
        dataOutput.writeUTF(deviceId);
        dataOutput.writeUTF(appkey);
        dataOutput.writeUTF(ip);
        dataOutput.writeLong(selfDuration);
        dataOutput.writeLong(thirdPartDuration);
        dataOutput.writeUTF(status);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.id = dataInput.readUTF();
        this.deviceId = dataInput.readUTF();
        this.appkey = dataInput.readUTF();
        this.ip = dataInput.readUTF();
        this.selfDuration = dataInput.readLong();
        this.thirdPartDuration = dataInput.readLong();
        this.status = dataInput.readUTF();
    }
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDeviceId() {
        return deviceId;
    }

    public void setDeviceId(String deviceId) {
        this.deviceId = deviceId;
    }

    public String getAppkey() {
        return appkey;
    }

    public void setAppkey(String appkey) {
        this.appkey = appkey;
    }

    public String getIp() {
        return ip;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public Long getSelfDuration() {
        return selfDuration;
    }

    public void setSelfDuration(Long selfDuration) {
        this.selfDuration = selfDuration;
    }

    public Long getThirdPartDuration() {
        return thirdPartDuration;
    }

    public void setThirdPartDuration(Long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }

    @Override
    public String toString() {
        return id + '\t' +
                "\t" + deviceId + '\t' + appkey + '\t' +
                ip + '\t' +
                selfDuration +
                "\t" + thirdPartDuration +
                "\t" + status;
    }
}
