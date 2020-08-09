package com.donaldy.mr.output;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/09
 */
public class OutputMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws
            IOException, InterruptedException {
        context.write(value, NullWritable.get());
    }
}