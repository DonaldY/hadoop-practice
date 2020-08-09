package com.donaldy.mr.writeable_demo;


import org.apache.hadoop.io.Writable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/04
 */
// 1. 实现writable接口口
public class SpeakBean implements Writable {
    private long selfDuration;
    private long thirdPartDuration;
    private long sumDuration;

    //2. 反序列列化时,需要反射调用用空参构造函数,所以必须有
    public SpeakBean() {
    }
    public SpeakBean(long selfDuration, long thirdPartDuration) {this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.sumDuration=this.selfDuration+this.thirdPartDuration;
    }

    //3. 写序列列化方方法
    public void write(DataOutput out) throws IOException {
        out.writeLong(selfDuration);
        out.writeLong(thirdPartDuration);
        out.writeLong(sumDuration);
    }

    //4. 反序列列化方方法
    //5. 反序列列化方方法读顺序必须和写序列列化方方法的写顺序必须一一致
    public void readFields(DataInput in) throws IOException {
        this.selfDuration
                = in.readLong();
        this.thirdPartDuration = in.readLong();
        this.sumDuration = in.readLong();
    }
    // 6. 编写toString方方法,方方便便后续打印到文文本
    @Override
    public String toString() {
        return
                selfDuration +
                        "\t" + thirdPartDuration +
                        "\t" + sumDuration ;
    }
    public long getSelfDuration() {
        return selfDuration;
    }
    public void setSelfDuration(long selfDuration) {
        this.selfDuration = selfDuration;
    }
    public long getThirdPartDuration() {
        return thirdPartDuration;
    }
    public void setThirdPartDuration(long thirdPartDuration) {
        this.thirdPartDuration = thirdPartDuration;
    }
    public long getSumDuration() {
        return sumDuration;
    }
    public void setSumDuration(long sumDuration) {
        this.sumDuration = sumDuration;
    }public void set(long selfDuration, long thirdPartDuration) {
        this.selfDuration = selfDuration;
        this.thirdPartDuration = thirdPartDuration;
        this.sumDuration=this.selfDuration+this.thirdPartDuration;
    }
}