package com.donaldy.mr.partition;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/13
 */

/**
 * 1. 读取一行文本，按照制表符切分
 * 2. 解析出 appley 字段， 其余数据封装为 PartitionBean 对象 (实现序列化 Writable 接口)
 * 3. 设计 map() 输出的 kv, key -> appkey(依靠该字段完成分区), PartitionBean对象作为 value 输出
 */
public class PartitionMapper extends Mapper<LongWritable, Text, Text, PartitionBean> {

    final PartitionBean bean = new PartitionBean();

    Text text = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        final String[] fields = value.toString().split("\t");

        String appkey = fields[2];
        bean.setId(fields[0]);
        bean.setDeviceId(fields[1]);
        bean.setAppkey(appkey);
        bean.setIp(fields[3]);
        bean.setSelfDuration(Long.parseLong(fields[4]));
        bean.setThirdPartDuration(Long.parseLong(fields[5]));
        bean.setStatus(fields[6]);

        // shuffle 开始时会根据 test 的hashcode进行分区, 但是结合业务, 默认 hash 分区方式不满足
        // 需要自定义写 partitioner
        text.set(appkey);context.write(text, bean);
    }
}
