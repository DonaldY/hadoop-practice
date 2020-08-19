package com.donaldy.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

/**
 * @author donald
 * @date 2020/08/19
 */
public class Nvl extends UDF {

    public Text evaluate(final Text t, final Text x) {
        if (t == null || t.toString().trim().length()==0) {
            return x;
        }
        return t;
    }
}