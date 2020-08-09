package com.donaldy.mr.writeable_demo;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;

/**
 * @author donald
 * @date 2020/08/04
 */
public class SpeakDurationReducer extends Reducer<Text, SpeakBean, Text, SpeakBean> {

    @Override
    protected void reduce(Text key, Iterable<SpeakBean> values, Context context)
            throws IOException, InterruptedException {
        long self_Duration = 0;
        long thirdPart_Duration = 0;

        // 1. 遍历所用用bean,将其中的自自有,第三方方时⻓长分别累加
        for (SpeakBean sb : values) {
            self_Duration += sb.getSelfDuration();
            thirdPart_Duration += sb.getThirdPartDuration();
        }

        // 2. 封装对象
        SpeakBean resultBean = new SpeakBean(self_Duration, thirdPart_Duration);

        // 3. 写出
        context.write(key, resultBean);
    }
}