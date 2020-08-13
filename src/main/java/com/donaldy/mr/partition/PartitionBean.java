package com.donaldy.mr.partition;

import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/13
 */
public class PartitionBean implements Writable {

    private String id;              // 日志id
    private String deviceId;        // 设备id
    private String appkey;          // appkey 合作硬件厂商id
    private String ip;              // ip地址
    private Long selfDuration;      // 自有内容时长
    private Long thirdPartDuration; // 第三方内容时长
    private String status;          // 状态码

    public PartitionBean() {
    }

    public PartitionBean(String id, String deviceId, String appkey, String ip,
                         Long selfDuration, Long thirdPartDuration, String status) {
        this.id = id;
        this.deviceId = deviceId;
        this.appkey = appkey;
        this.ip = ip;
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.status = status;
    }

    public String getId() {
        return id;
    }
    public void setId(String id) {
        this.id = id;
    }
    public String getDeviceId() {
        return deviceId;}
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
    //序列化
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(id);
        out.writeUTF(deviceId);
        out.writeUTF(appkey);
        out.writeUTF(ip);
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeUTF(status);
    }
    //反序列化
    @Override
    public void readFields(DataInput in) throws IOException {
        this.id = in.readUTF();
        this.deviceId=in.readUTF();
        this.appkey=in.readUTF();
        this.ip=in.readUTF();
        this.selfDuration=in.readLong();
        this.thirdPartDuration=in.readLong();
        this.status=in.readUTF();
    }
    @Override
    public String toString() {
        return id + '\t' +
                deviceId + '\t' +
                appkey + '\t' +
                ip + '\t' +
                selfDuration +'\t'+
                thirdPartDuration +
                '\t' + status ;
    }
}
