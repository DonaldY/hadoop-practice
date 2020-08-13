## 一、作业需求

`MapReduce` 程序读取这三个文件, 对三个文件中的数字进行整体升序排序, 并输出到一个结果文件中。
结果文件中的每一行有两个数字(两个数字之间使用制表符分隔), 第一个数字代表排名, 第二个数字代表原始数据。

1. 输入
`file1.txt`
```text
2
32
654
32
15
756
65223
```

`file2.txt`
```text
5956
22
650
92
```

`file3.txt`
```text
26
54
6
```


2. 输出
```text
1 2
2 6
3 15
4 22
5 26
6 32
7 32
8 54
9 9210 650
11 654
12 756
13 5956
14 65223
```


## 二、作业思路

### (1) 解题思路

1. 整体升序排序, 一个结果文件
> 说明只能有一个 `reduceTask`


### (2) 步骤


0. 比较器
```java
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
```



1. `Mapper`
```java
package com.donaldy.mr.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/13
 */
public class NumberMapper extends Mapper<LongWritable, Text, IntWritable, NullWritable> {


    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        final String field = value.toString();

        IntWritable intWritable = new IntWritable(Integer.parseInt(field));

        context.write(intWritable, NullWritable.get());
    }
}
```


2. `Reducer`
```java
package com.donaldy.mr.homework;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


/**
 * @author donald
 * @date 2020/08/13
 */
public class NumberReducer extends Reducer<IntWritable, NullWritable, IntWritable, IntWritable> {

    int num = 1;

    IntWritable intWritable = new IntWritable();

    @Override
    protected void reduce(IntWritable key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {

        for (NullWritable value : values) {

            intWritable.set(num++);

            context.write(intWritable, key);
        }
    }
}
```


3. `Driver`
```java
package com.donaldy.mr.homework;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/13
 */
public class NumberDriver {

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 1. 获取配置文件对象，获取job对象实例
        final Configuration conf = new Configuration();

        final Job job = Job.getInstance(conf, "NumberDriver");
        // 2. 指定程序jar的本地路径
        job.setJarByClass(NumberDriver.class);
        // 3. 指定Mapper/Reducer类
        job.setMapperClass(NumberMapper.class);
        job.setReducerClass(NumberReducer.class);
        // 4. 指定Mapper输出的kv数据类型
        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(NullWritable.class);
        // 5. 指定最终输出的kv数据类型
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(IntWritable.class);

        // 6. 排序
        job.setSortComparatorClass(NumberComparator.class);
        FileInputFormat.setInputPaths(job, new Path("/home/donald/Documents/demo/homework"));
        // 7. 指定job输出结果路径
        FileOutputFormat.setOutputPath(job, new Path("/home/donald/Documents/demo/output/homework"));

        // 8. 提交作业
        final boolean flag = job.waitForCompletion(true);

        //jvm退出：正常退出0，非0值则是错误退出
        System.exit(flag ? 0 : 1);

    }
}
```



### (3) 输出结果

```bash
donald@donald-pro:~/Documents/demo/output/homework$ ll
total 20
drwxrwxr-x 2 donald donald 4096 Aug 13 20:44 ./
drwxrwxr-x 7 donald donald 4096 Aug 13 20:44 ../
-rw-r--r-- 1 donald donald   81 Aug 13 20:44 part-r-00000
-rw-r--r-- 1 donald donald   12 Aug 13 20:44 .part-r-00000.crc
-rw-r--r-- 1 donald donald    0 Aug 13 20:44 _SUCCESS
-rw-r--r-- 1 donald donald    8 Aug 13 20:44 ._SUCCESS.crc
donald@donald-pro:~/Documents/demo/output/homework$ cat part-r-00000 
1	2
2	6
3	15
4	22
5	26
6	32
7	32
8	54
9	92
10	650
11	654
12	756
13	5956
14	65223
```