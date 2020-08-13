package com.donaldy.mr.sort;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


/**
 * @author donald
 * @date 2020/08/09
 */
public class SortMapper extends Mapper<LongWritable, Text, SpeakBean, NullWritable> {
    final SpeakBean bean = new SpeakBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //1 读取一行文本，转为字符串，切分
        final String[] fields = value.toString().split(" ");
        //2 解析出各个字段封装成SpeakBean对象
        bean.setDeviceId(fields[0]);
        bean.setSelfDrutation(Long.parseLong(fields[1]));
        bean.setThirdPartDuration(Long.parseLong(fields[2]));
        bean.setSumDuration(Long.parseLong(fields[4]));
        //3 SpeakBean作为key输出
        context.write(bean, NullWritable.get());
    }
}