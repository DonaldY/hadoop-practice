package com.donaldy.zk.demo;

import org.I0Itec.zkclient.ZkClient;

/**
 * @author donald
 * @date 2020/08/27
 */
public class CreateNodeSample {

    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient("172.16.64.121:2181");

        System.out.println("ZooKeeper session established.");

        //createParents的值设置为true,可以递归创建节点
        zkClient.createPersistent("/zkClient/c1",true);

        System.out.println("success create znode.");
    }
}