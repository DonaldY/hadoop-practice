package com.donaldy.mr.partition;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/13
 */
// reduce 输入类型： Text, PartitionBean; 输出: Text, PartitionBean
public class PartitionReducer extends Reducer<Text, PartitionBean, NullWritable, PartitionBean> {

    @Override
    protected void reduce(Text key, Iterable<PartitionBean> values, Context context)
            throws IOException, InterruptedException {

        // 无需聚合运算，只需要进行输出即可
        for (PartitionBean bean : values) {

            context.write(NullWritable.get(), bean);
        }
    }
}