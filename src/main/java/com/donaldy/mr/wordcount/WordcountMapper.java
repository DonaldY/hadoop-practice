package com.donaldy.mr.wordcount;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author donald
 * @date 2020/08/03
 */
public class WordcountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

    // 提升为全局变量，避免每次执行map方法都执行此操作
    Text k = new Text();
    IntWritable v = new IntWritable(1);

    // LongWritable, Text-->文本偏移量，一行文本内容，map方法的输入参数，一行文本就调用一次map方法
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        // 1. 获取一行
        String line = value.toString();

        // 2. 切割
        String[] words = line.split(" ");

        // 3. 输出
        for (String word : words) {
            k.set(word);
            context.write(k, v);
        }
    }
}