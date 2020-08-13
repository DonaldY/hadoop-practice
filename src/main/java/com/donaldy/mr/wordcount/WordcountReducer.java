package com.donaldy.mr.wordcount;


import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author donald
 * @date 2020/08/03
 */
public class WordcountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    int sum;

    IntWritable v = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException,
            InterruptedException {

        // 1. 累加求和
        sum = 0;

        for (IntWritable count : values) {
            sum += count.get();
        }

        // 2. 输出
        v.set(sum);
        context.write(key,v);
    }
}
