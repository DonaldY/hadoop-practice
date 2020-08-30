package com.donaldy.zk.demo;

import org.I0Itec.zkclient.IZkChildListener;
import org.I0Itec.zkclient.ZkClient;
import java.util.List;

/**
 * 演示 zkClient 使用监听器
 *
 * @author donald
 * @date 2020/08/27
 */
public class GetChildChange {

    public static void main(String[] args) throws InterruptedException {

        // 获取到zkClient
        final ZkClient zkClient = new ZkClient("172.16.64.121:2181");

        // zkClient 对指定目录进行监听(不存在目录:/lg-client), 指定收到通知之后的逻辑

        // 对 /zkClient 注册了监听器, 监听器是一直监听
        // 该方法是接收到通知之后的执行逻辑定义
        zkClient.subscribeChildChanges("/zkClient",
                (path, childes) -> System.out.println(path + " childes changes ,current childes " + childes));

        // 使用zkClient创建节点, 删除节点, 验证监听器是否运行
        zkClient.createPersistent("/zkClient");
        Thread.sleep(1000);

        //只是为了了方方便便观察结果数据
        zkClient.createPersistent("/zkClient/c1");
        Thread.sleep(1000);

        zkClient.delete("/zkClient/c1");
        Thread.sleep(1000);

        zkClient.delete("/zkClient");
        Thread.sleep(2000);

        /**
         * 1 监听器可以对不存在的目录进行监听
         * 2 监听目录下子节点发生改变,可以接收到通知,携带数据有子节点列表
         * 3 监听目录创建和删除本身也会被监听到
         */
    }
}