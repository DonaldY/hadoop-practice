package com.donaldy.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/13
 */
public class SortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        /**
         * 1. 获取配置文件对象，获取job对象实例
         * 2. 指定程序jar的本地路径
         * 3. 指定Mapper/Reducer类
         * 4. 指定Mapper输出的kv数据类型
         * 5. 指定最终输出的kv数据类型
         * 6. 指定job处理的原始数据路径
         * 7. 指定job输出结果路径
         * 8. 提交作业
         */
        // 1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "SortDriver");
        // 2. 指定程序jar的本地路径
        job.setJarByClass(SortDriver.class);
        // 3. 指定Mapper/Reducer类
        job.setMapperClass(SortMapper.class);
        job.setReducerClass(SortReducer.class);
        // 4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(SpeakBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(SpeakBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定reduceTask的数量，默认是1个
        job.setNumReduceTasks(1);
        // 6. 指定job处理的原始数据路径
        //import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
        //import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
        FileInputFormat.setInputPaths(job, new Path("/home/donald/Documents/demo/sort.txt")); //指定读取数据的原始路径
        // 7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path("/home/donald/Documents/demo/output/sort")); //指定结果数据输出路径
        // 8. 提交作业
        final boolean flag = job.waitForCompletion(true);

        // jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}