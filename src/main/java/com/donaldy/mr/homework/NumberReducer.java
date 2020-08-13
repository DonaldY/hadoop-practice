package com.donaldy.mr.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author donald
 * @date 2020/08/13
 */
public class NumberReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

    int num = 1;

    IntWritable intWritable = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {

            intWritable.set(num++);

            context.write(intWritable, key);
        }
    }
}
