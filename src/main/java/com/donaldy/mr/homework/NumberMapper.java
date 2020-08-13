package com.donaldy.mr.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/13
 */
public class NumberMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        final String field = value.toString();

        IntWritable intWritable = new IntWritable(Integer.parseInt(field));

        context.write(intWritable, NullWritable.get());
    }
}
