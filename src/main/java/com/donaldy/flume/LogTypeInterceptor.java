package com.donaldy.flume;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.compress.utils.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.interceptor.Interceptor;import org.junit.Test;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author donald
 * @date 2020/10/06
 */
public class LogTypeInterceptor implements Interceptor {

    @Override
    public void initialize() {
    }

    // 逐条处理event
    @Override
    public Event intercept(Event event) {

        // 获取 event 的 body
        String eventBody = new String(event.getBody(), Charsets.UTF_8);
        // 获取 event 的 header
        Map<String, String> headersMap = event.getHeaders();
        // 解析body获取json串
        String[] bodyArr = eventBody.split("\\s+");
        try{
            String jsonStr = bodyArr[6];
            // 解析json串获取时间戳
            String timestampStr = "";
            JSONObject jsonObject = JSON.parseObject(jsonStr);
            if (headersMap.getOrDefault("logtype", "").equals("start")){
                // 取启动日志的时间戳
                timestampStr = jsonObject.getJSONObject("app_active").getString("time");
            } else if (headersMap.getOrDefault("logtype", "").equals("event")) {
                // 取事件日志第一条记录的时间戳
                JSONArray jsonArray = jsonObject.getJSONArray("lagou_event");
                if (jsonArray.size() > 0){
                    timestampStr =
                            jsonArray.getJSONObject(0).getString("time");
                }
            }
            // 将时间戳转换为字符串 "yyyy-MM-dd"
            // 将字符串转换为Long
            long timestamp = Long.parseLong(timestampStr);
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
            Instant instant = Instant.ofEpochMilli(timestamp);
            LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
            String date = formatter.format(localDateTime);
            // 将转换后的字符串放置header中
            headersMap.put("logtime", date);
            event.setHeaders(headersMap);
        }catch (Exception e){
            headersMap.put("logtime", "Unknown");
            event.setHeaders(headersMap);
        }
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        List<Event> lstEvent = new ArrayList<>();
        for (Event event: events){
            Event outEvent = intercept(event);
            if (outEvent != null) {
                lstEvent.add(outEvent);
            }
        }
        return lstEvent;
    }

    @Override
    public void close() {
    }

    public static class Builder implements Interceptor.Builder {
        @Override
        public Interceptor build() {
            return new LogTypeInterceptor();
        }
        @Override
        public void configure(Context context) {
        }
    }

    @Test
    public void startJunit(){
        String str = "2020-08-02 18:19:32.959 [main] INFO com.lagou.ecommerce.AppStart - {\"app_active\": {\"name\":\"app_active\",\"json\": {\"entry\":\"1\",\"action\":\"0\",\"error_code\":\"0\"},\"time\":1596342840284},\"attr\":{\"area\":\"大庆\",\"uid\":\"2F10092A2\",\"app_v\":\"1.1.15\",\"event_type\":\"common\",\"device_id\":\"1FB872-9A1002\",\"os_type\":\"2.8\",\"channel\":\"TB\",\"language\":\"chinese\",\"brand\":\"iphone-8\"}}";
        Map<String, String> map = new HashMap<>();
        // new Event
        Event event = new SimpleEvent();
        map.put("logtype", "start");
        event.setHeaders(map);
        event.setBody(str.getBytes(Charsets.UTF_8));
        // 调用interceptor处理event
        LogTypeInterceptor customerInterceptor = new LogTypeInterceptor();
        Event outEvent = customerInterceptor.intercept(event);
        // 处理结果
        Map<String, String> headersMap = outEvent.getHeaders();
        System.out.println(JSON.toJSONString(headersMap));
    }

    @Test
    public void eventJunit(){
        String str = "2020-08-02 18:20:11.877 [main] INFO com.lagou.ecommerce.AppEvent - {\"lagou_event\": [{\"name\":\"goods_detail_loading\",\"json\": {\"entry\":\"1\",\"goodsid\":\"0\",\"loading_time\":\"93\",\"acti on\":\"3\",\"staytime\":\"56\",\"showtype\":\"2\"},\"time\":15963 43881690},{\"name\":\"loading\",\"json\": {\"loading_time\":\"15\",\"action\":\"3\",\"loading_type\":\"3\",\"type\":\"1\"},\"time\":1596356988428}, {\"name\":\"notification\",\"json\": {\"action\":\"1\",\"type\":\"2\"},\"time\":1596374167278}, {\"name\":\"favorites\",\"json\": {\"course_id\":1,\"id\":0,\"userid\":0},\"time\":1596350933962}],\"attr\":{\"area\":\"长治\",\"uid\":\"2F10092A4\",\"app_v\":\"1.1.14\",\"event_type\":\"common\",\"device_id\":\"1FB872- 9A1004\",\"os_type\":\"0.5.0\",\"channel\":\"QL\",\"language\":\" chinese\",\"brand\":\"xiaomi-0\"}}";
        Map<String, String> map = new HashMap<>();
        // new Event
        Event event = new SimpleEvent();
        map.put("logtype", "event");
        event.setHeaders(map);
        event.setBody(str.getBytes(Charsets.UTF_8));
        // 调用interceptor处理event
        LogTypeInterceptor customerInterceptor = new
                LogTypeInterceptor();
        Event outEvent = customerInterceptor.intercept(event);
        // 处理结果
        Map<String, String> headersMap = outEvent.getHeaders();
        System.out.println(JSON.toJSONString(headersMap));
    }
}
