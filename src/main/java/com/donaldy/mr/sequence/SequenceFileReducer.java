package com.donaldy.mr.sequence;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/09
 */
public class SequenceFileReducer extends Reducer<Text, BytesWritable,Text,BytesWritable> {

    @Override
    protected void reduce(Text key, Iterable<BytesWritable> values, Context
            context) throws IOException, InterruptedException {

        // 输出value值, 其中只有一个 BytesWritable 所以直接next取出即可
        context.write(key, values.iterator().next());
    }
}
