package com.donaldy.mr.sequence;

/**
 * @author donald
 * @date 2020/08/09
 */
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import java.io.IOException;
public class SequenceFileDriver {

    public static void main(String[] args) throws IOException,
            ClassNotFoundException, InterruptedException {

        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf);

        job.setJarByClass(SequenceFileDriver.class);

        job.setMapperClass(SequenceFileMapper.class);

        job.setReducerClass(SequenceFileReducer.class);

        job.setMapOutputKeyClass(Text.class);

        job.setMapOutputValueClass(BytesWritable.class);

        job.setOutputKeyClass(Text.class);

        job.setOutputValueClass(BytesWritable.class);

        job.setInputFormatClass(CustomFileInputFormat.class);

        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.setInputPaths(job, new Path(args[0]));

        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        final boolean flag = job.waitForCompletion(true);

        System.exit(flag ? 0 : 1);
    }
}
