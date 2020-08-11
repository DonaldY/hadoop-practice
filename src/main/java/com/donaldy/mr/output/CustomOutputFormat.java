package com.donaldy.mr.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/09
 */
public class CustomOutputFormat extends FileOutputFormat<Text, NullWritable> {

    @Override
    public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context)
            throws IOException, InterruptedException {

        //获取文件系统对象
        final FileSystem fs = FileSystem.get(context.getConfiguration());

        //指定输出数据的文件
        final Path lagouPath = new Path("e:/lagou.log");

        final Path otherLog = new Path("e:/other.log");

        //获取输出流
        final FSDataOutputStream lagouOut = fs.create(lagouPath);
        final FSDataOutputStream otherOut = fs.create(otherLog);

        return new CustomWriter(lagouOut, otherOut);
    }
}