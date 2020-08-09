package com.donaldy.mr.sequence;

import org.apache.hadoop.io.BytesWritable;import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/09
 */
public class SequenceFileMapper extends Mapper<Text, BytesWritable,Text,BytesWritable> {

    @Override
    protected void map(Text key, BytesWritable value, Context context) throws
            IOException, InterruptedException {

        // 读取内容直接输出
        context.write(key, value);
    }
}