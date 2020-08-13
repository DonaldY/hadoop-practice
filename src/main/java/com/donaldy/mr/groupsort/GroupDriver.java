package com.donaldy.mr.groupsort;


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
public class GroupDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
         /*
        1. 获取配置文件对象，获取job对象实例
        2. 指定程序jar的本地路径
        3. 指定Mapper/Reducer类
        4. 指定Mapper输出的kv数据类型
        5. 指定最终输出的kv数据类型
        6. 指定job处理的原始数据路径
        7. 指定job输出结果路径
        8. 提交作业
         */
        // 1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "GroupDriver");
        // 2. 指定程序jar的本地路径
        job.setJarByClass(GroupDriver.class);
        // 3. 指定Mapper/Reducer类
        job.setMapperClass(GroupMapper.class);
        job.setReducerClass(GroupReducer.class);
        // 4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);

        //指定分区器
        job.setPartitionerClass(CustomPartitioner.class);
        //指定使用groupingcomparator
        job.setGroupingComparatorClass(CustomGroupingComparator.class);
        FileInputFormat.setInputPaths(job, new Path("/home/donald/Documents/demo/groupsort.txt")); //指定读取数据的原始路径
        // 7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path("/home/donald/Documents/demo/output/group")); //指定结果数据输出路径

        //指定reducetask的数量，不要使用默认的一个，分区效果不明显
        job.setNumReduceTasks(2);
        // 8. 提交作业
        final boolean flag = job.waitForCompletion(true);

        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}
