package com.donaldy.mr.sequence;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/09
 */
public class CustomRecordReader extends RecordReader<Text, BytesWritable> {

    private Configuration configuration;
    // 切片
    private FileSplit split;
    // 是否读取到内容的标识符
    private boolean isProgress = true;
    // 输出的kv
    private BytesWritable value = new BytesWritable();

    private Text k = new Text();

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {

        // 获取到文件切片以及配置文件对象
        this.split = (FileSplit) split;
        configuration = context.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if (isProgress) {

            // 1. 定义缓存区
            byte[] contents = new byte[(int) split.getLength()];
            FileSystem fs = null;
            FSDataInputStream fis = null;

            try {
                // 2. 获取文件系统
                Path path = split.getPath();
                fs = path.getFileSystem(configuration);
                // 3. 读取数据
                fis = fs.open(path);
                // 4. 读取文文件内容
                IOUtils.readFully(fis, contents, 0, contents.length);
                // 5. 输出文文件内容
                value.set(contents, 0, contents.length);
                // 6. 获取文文件路路径及名称
                String name = split.getPath().toString();
                // 7. 设置输出的key值
                k.set(name);

            } catch (Exception e) {
            } finally {
                IOUtils.closeStream(fis);
            }
            isProgress = false;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return k;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
    }
}
