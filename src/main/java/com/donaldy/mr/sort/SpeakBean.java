package com.donaldy.mr.sort;


import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

/**
 * @author donald
 * @date 2020/08/13
 */
// 因为这个类的实例对象要作为map输出的key，所以要实现 writablecomparalbe 接口
public class SpeakBean implements WritableComparable<SpeakBean> {

    private Long selfDrutation;     //自有内容播放时长
    private Long thirdPartDuration; //第三方内容播放时长
    private String deviceId;        //设备id
    private Long sumDuration;       //总时长

    public SpeakBean() {
    }

    public SpeakBean(Long selfDrutation, Long thirdPartDuration, String deviceId, Long sumDuration) {
        this.selfDrutation = selfDrutation;
        this.thirdPartDuration = thirdPartDuration;
        this.deviceId = deviceId;
        this.sumDuration = sumDuration;
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


    //序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDrutation);
        out.writeLong(thirdPartDuration);
        out.writeUTF(deviceId);
        out.writeLong(sumDuration);
    }

    //反序列化方法
    @Override
    public void readFields(DataInput in) throws IOException {
        this.selfDrutation = in.readLong();
        this.thirdPartDuration = in.readLong();
        this.deviceId = in.readUTF();
        this.sumDuration = in.readLong();
    }

    //指定排序规则,我们希望按照总时长进行排序
    @Override
    public int compareTo(SpeakBean o) {

        //返回值三种：0：相等 1：小于 -1：大于
        //指定按照bean对象的总时长字段的值进行比较
        if (this.sumDuration > o.sumDuration) {
            return -1;
        } else if (this.sumDuration < o.sumDuration) {
            return 1;
        } else {
            return 0; //加入第二个判断条件，二次排序
        }

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

    @Override
    public String toString() {
        return selfDrutation +
                "\t" + thirdPartDuration +
                "\t" + deviceId + '\t' +
                sumDuration
                ;
    }
}