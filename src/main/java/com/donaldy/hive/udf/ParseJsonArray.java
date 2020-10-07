package com.donaldy.hive.udf;

/**
 * @author donald
 * @date 2020/10/06
 */
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONException;
import com.alibaba.fastjson.JSONObject;
import com.google.common.base.Strings;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.junit.Test;
import java.util.ArrayList;
public class ParseJsonArray extends UDF {

    public ArrayList<String> evaluate(String jsonStr, String arrKey){
        if (Strings.isNullOrEmpty(jsonStr)) {
            return null;
        }

        try{
            JSONObject object = JSON.parseObject(jsonStr);
            JSONArray jsonArray = object.getJSONArray(arrKey);
            ArrayList<String> result = new ArrayList<>();
            for (Object o: jsonArray){
                result.add(o.toString());
            }return result;
        } catch (JSONException e){
            return null;
        }
    }

    @Test
    public void JunitParseJsonArray(){
        String str = "{\"id\": 1,\"ids\":[101,102,103],\"total_number\": 3}";
        String key = "ids";
        ArrayList<String> evaluate = evaluate(str, key);
        System.out.println(JSON.toJSONString(evaluate));
    }
}
