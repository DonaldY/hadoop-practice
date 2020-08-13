package com.donaldy.mr.sort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/13
 */
public class SortReducer extends Reducer<SpeakBean, NullWritable, SpeakBean, NullWritable> {
    //reduce方法的调用是相同key的value组成一个集合调用一次
    /**
     * java中如何判断两个对象是否相等？
     * 根据equals方法，比较还是地址值
     */
    @Override
    protected void reduce(SpeakBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
        // 讨论按照总流量排序这件事情，还需要在reduce端处理吗？
        // 因为之前已经利用mr的shuffle对数据进行了排序
        // 为了避免前面compareTo方法导致总流量相等被当成对象相等，而合并了key，所以遍历values获取每个key（bean对象）
        for (NullWritable value : values) {
            //遍历value同时，key也会随着遍历。
            context.write(key, value);
        }
    }
}