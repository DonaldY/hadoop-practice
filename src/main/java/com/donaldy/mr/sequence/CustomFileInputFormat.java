package com.donaldy.mr.sequence;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/09
 */
// 自定义 inputFormat： key -> 文件路径 + 名称, value -> 整个文件内容
public class CustomFileInputFormat extends FileInputFormat<Text, BytesWritable> {

    // 文件不可切分
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }

    // 获取自定义 RecordReader 对象用来读取数据
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        CustomRecordReader recordReader = new CustomRecordReader();
        recordReader.initialize(split, context);
        return recordReader;
    }
}
