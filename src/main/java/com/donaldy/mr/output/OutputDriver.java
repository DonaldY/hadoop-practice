package com.donaldy.mr.output;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/09
 */
public class OutputDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf);

        job.setJarByClass(OutputDriver.class);

        job.setMapperClass(OutputMapper.class);

        job.setReducerClass(OutputReducer.class);

        job.setMapOutputKeyClass(Text.class);job.setMapOutputValueClass(NullWritable.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(NullWritable.class);

        job.setOutputFormatClass(CustomOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(""));

        // outputformat继承自fileoutputformat 而fileoutputformat要输出一个
        // _SUCCESS文件,所以,在这还得指定一个输出目录

        FileOutputFormat.setOutputPath(job, new Path(""));

        final boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}