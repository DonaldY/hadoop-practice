package com.donaldy.mr.partition;


import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;
/**
 * @author donald
 * @date 2020/08/13
 */
// Partitioner 分区器的范型是 map 输出的 kv类型
public class CustomPartitioner extends Partitioner<Text,PartitionBean> {
    @Override
    public int getPartition(Text text, PartitionBean partitionBean, int numPartitions) {

        int partition=0;

        final String appkey = text.toString();

        if(appkey.equals("kar")) {
            // 只需要保证满足此 if 条件的数据, 获得同个分区编号集合
            partition = 1;
        }else if(appkey.equals("pandora")) {
            partition = 2;
        }

        return partition;
    }
}
