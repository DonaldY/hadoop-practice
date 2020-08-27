package com.donaldy.zk.demo;

import org.I0Itec.zkclient.ZkClient;

/**
 * @author donald
 * @date 2020/08/27
 */
public class DeleteNodeSample {

    public static void main(String[] args) {

        ZkClient zkClient = new ZkClient("172.16.64.121:2181");

        String path = "/zkClient/c1";

        // 表明ZkClient可直接删除带子节点的父节点,因为其底层先删除其所有子节点,然后再删除父节点
        // 递归删除
        zkClient.deleteRecursive(path);

        System.out.println("success delete znode.");
    }
}
