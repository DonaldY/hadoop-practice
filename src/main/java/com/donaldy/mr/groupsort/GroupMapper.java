package com.donaldy.mr.groupsort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/13
 */
public class GroupMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean bean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        final String[] fields = value.toString().split("\t");
        //订单id与jine封装为一个orderBean
        bean.setOrderId(fields[0]);
        bean.setPrice(Double.parseDouble(fields[2]));
        context.write(bean, NullWritable.get());
    }
}
