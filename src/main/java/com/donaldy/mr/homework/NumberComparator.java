package com.donaldy.mr.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author donald
 * @date 2020/08/13
 */
public class NumberComparator extends WritableComparator {

    public NumberComparator() {
        super(IntWritable.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {

        final IntWritable o1 = (IntWritable) a;
        final IntWritable o2 = (IntWritable) b;

        return o1.compareTo(o2);
    }
}
