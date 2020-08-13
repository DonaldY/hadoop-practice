package com.donaldy.mr.output;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/09
 */
public class CustomWriter extends RecordWriter<Text, NullWritable> {
    private FSDataOutputStream lagouOut;
    private FSDataOutputStream otherOut;

    public CustomWriter(FSDataOutputStream lagouOut, FSDataOutputStream otherOut) {
        this.lagouOut=lagouOut;
        this.otherOut=otherOut;
    }

    @Override
    public void write(Text key, NullWritable value) throws IOException, InterruptedException {

        // 判断是否包含“haha”输出到不同文件
        if (key.toString().contains("haha")) {
            lagouOut.write(key.toString().getBytes());
            lagouOut.write("\r\n".getBytes());

        } else {
            otherOut.write(key.toString().getBytes());
            otherOut.write("\r\n".getBytes());
        }
    }
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
        IOUtils.closeStream(lagouOut);
        IOUtils.closeStream(otherOut);
    }
}
