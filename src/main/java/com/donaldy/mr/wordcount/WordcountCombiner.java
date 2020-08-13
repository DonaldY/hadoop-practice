package com.donaldy.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
/**
 * @author donald
 * @date 2020/08/13
 */
// combiner 组件的输入和输出类型与 map() 方法保持一致
public class WordcountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable total = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context
            context) throws IOException, InterruptedException {

        // 进行局部汇总，逻辑是与reduce方法保持一致

        // 2. 遍历key对应的values, 然后累加结果
        int sum = 0;
        for (IntWritable value : values) {
            int i = value.get();
            sum += 1;
        }

        // 3. 直接输出当前key对应的sum值, 结果就是单词出现的总次数
        total.set(sum);
        context.write(key, total);
    }
}
