package com.donaldy.zk.demo;

import java.io.IOException;
import org.I0Itec.zkclient.ZkClient;

/**
 * @author donald
 * @date 2020/08/27
 */
public class CreateSession {

    /**
     * 创建一个 zkClient 实例来进行连接
     *
     * @param args 参数
     */
    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient("172.16.64.121:2181");

        System.out.println("ZooKeeper session created.");
    }
}
