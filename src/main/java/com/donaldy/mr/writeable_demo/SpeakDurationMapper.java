package com.donaldy.mr.writeable_demo;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;


/**
 * @author donald
 * @date 2020/08/04
 */
public class SpeakDurationMapper extends Mapper<LongWritable, Text, Text, SpeakBean> {

    SpeakBean v = new SpeakBean();

    Text k = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1. 获取一行
        String line = value.toString();

        // 2. 切割字段
        String[] fields = line.split("\t");

        // 3. 封装对象
        // 取出设备id
        String deviceId = fields[1];

        //取出自自有和第三方方时⻓长数据
        long selfDuration = Long.parseLong(fields[fields.length - 3]);
        long thirdPartDuration = Long.parseLong(fields[fields.length - 2]);
        k.set(deviceId);
        v.set(selfDuration, thirdPartDuration);

        // 4. 写出
        context.write(k, v);}
}
