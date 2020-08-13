package com.donaldy.mr.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/13
 */
public class NumberDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "NumberDriver");
        // 2. 指定程序jar的本地路径
        job.setJarByClass(NumberDriver.class);
        // 3. 指定Mapper/Reducer类
        job.setMapperClass(NumberMapper.class);
        job.setReducerClass(NumberReducer.class);
        // 4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 排序
        job.setSortComparatorClass(NumberComparator.class);
        FileInputFormat.setInputPaths(job, new Path("/home/donald/Documents/demo/homework"));
        // 7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path("/home/donald/Documents/demo/output/homework"));

        // 8. 提交作业
        final boolean flag = job.waitForCompletion(true);

        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}
