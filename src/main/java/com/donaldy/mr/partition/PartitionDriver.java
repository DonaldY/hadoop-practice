package com.donaldy.mr.partition;

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
 * @date 2020/08/13
 */
public class PartitionDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置文件
        final Configuration conf = new Configuration();

        // 2. 获取 job 实例
        final Job job = Job.getInstance(conf);
        // 3. 设置任务相关参数
        job.setJarByClass(PartitionDriver.class);
        job.setMapperClass(PartitionMapper.class);
        job.setReducerClass(PartitionReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PartitionBean.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(PartitionBean.class);

        // 4. 设置使用自定义分区器
        job.setPartitionerClass(CustomPartitioner.class);

        // 5. ReduceTask 数量, 指定 reduceTask 的数量与分区数量保持一致, 分区数量是 3
        job.setNumReduceTasks(3);
        // 6. 指定输入 和 输出数据路径
        FileInputFormat.setInputPaths(job, new Path("/home/donald/Documents/demo/speak.data"));
        FileOutputFormat.setOutputPath(job, new Path("/home/donald/Documents/demo/output/speak"));

        // 7. 提交任务
        final boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}
