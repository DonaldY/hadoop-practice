package com.donaldy.zk.watch;


import org.I0Itec.zkclient.ZkClient;

/**
 * @author donald
 * @date 2020/08/27
 */
public class ServerMain {

    private ZkClient zkClient = null;

    /**
     * 获取到zk对象
     */
    private void connectZK(){

        zkClient = new ZkClient("172.16.64.121:2181,172.16.64.122:2181,172.16.64.123:2181");

        if(!zkClient.exists("/servers")){

            zkClient.createPersistent("/servers");
        }
    }

    /**
     * 注册服务端信息到zk节点
     *
     * @param ip ip
     * @param port 端口
     */
    private void registerServerInfo(String ip, String port){

        //创建临时顺序节点
        final String path = zkClient.createEphemeralSequential("/servers/server", ip +":"+port);

        System.out.println("---->>> 服务器器注册成功,ip="+ip+";port ="+port+";节点路径信息="+path);
    }

    public static void main(String[] args) {

        final ServerMain server = new ServerMain();

        server.connectZK();

        server.registerServerInfo(args[0], args[1]);

        // 启动一个服务线程提供时间查询
        new TimeServer(Integer.parseInt(args[1])).start();
    }
}