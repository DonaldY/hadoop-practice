package com.donaldy.mapreduce.writeable_demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/04
 */
public class SpeakerDriver {
    public static void main(String[] args) throws
            IllegalArgumentException, IOException, ClassNotFoundException,
            InterruptedException {
        // 输入入输出路路径需要根据自自己己电脑上实际的输入入输出路路径设置
        args = new String[] { "e:/input/input", "e:/output1" };

        // 1. 获取配置信息,或者job对象实例例
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        // 6. 指定本程序的jar包所在的本地路路径
        job.setJarByClass(SpeakerDriver.class);

        // 2. 指定本业务job要使用用的mapper/Reducer业务类
        job.setMapperClass(SpeakDurationMapper.class);
        job.setReducerClass(SpeakDurationReducer.class);

        // 3. 指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(SpeakBean.class);

        // 4. 指定最终输出的数据的kv类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SpeakBean.class);

        // 5. 指定job的输入入原始文文件所在目目录
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // 7. 将job中配置的相关参数,以及job所用用的java类所在的jar包, 提交给yarn去运行
        boolean result = job.waitForCompletion(true);
        System.exit(result ? 0 : 1);
    }
}
