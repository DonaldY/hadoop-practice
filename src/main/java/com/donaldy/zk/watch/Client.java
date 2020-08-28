package com.donaldy.zk.watch;


import org.I0Itec.zkclient.ZkClient;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * 注册监听zk指定目录
 * 维护自己本地一个servers信息, 收到通知要进行更新
 * 发送时间查询请求并接受服务端返回的数据
 *
 * @author donald
 * @date 2020/08/27
 */
public class Client {

    private ZkClient zkClient = null;

    private List<String> infos = new ArrayList<>();

    private void connectZk() {

        zkClient = new ZkClient("172.16.64.121:2181,172.16.64.122:2181");

        final List<String> childes = zkClient.getChildren("/servers");

        for (String child : childes) {

            final Object o = zkClient.readData("/servers/" + child);

            infos.add(String.valueOf(o));
        }

        zkClient.subscribeChildChanges("/servers", (s, children) -> {

            // 接收到通知, 说明节点发生了变化, client需要更新 infos 集合中的数据
            List<String> list = new ArrayList<>();

            // 遍历更更新过后的所有节点信息
            for (String path : children) {
                final Object o = zkClient.readData("/servers/" + path);
                list.add(String.valueOf(o));
            }

            // 最新数据覆盖老数据
            infos = list;
            System.out.println("--》接收到通知,最新服务器器信息为:" + infos);
        });
    }

    /**
     * 发送时间查询的请求
     *
     * @throws IOException 异常
     */
    private void sendRequest() throws IOException {

        final Random random = new Random();
        final int i = random.nextInt(infos.size());
        final String ipPort = infos.get(i);
        final String[] arr = ipPort.split(":");

        final Socket socket = new Socket(arr[0], Integer.parseInt(arr[1]));
        final OutputStream out = socket.getOutputStream();
        final InputStream in = socket.getInputStream();
        out.write("query time".getBytes());
        out.flush();
        final byte[] b = new byte[1024];
        in.read(b);

        System.out.println("client端接收到server:+" + ipPort + "+返回结果:" + new String(b));

        in.close();
        out.close();
        socket.close();
    }
    public static void main(String[] args) throws InterruptedException {
        final Client client = new Client();
        client.connectZk();

        // 监听器逻辑
        while (true) {
            try {
                client.sendRequest(); //发送请求
            } catch (IOException e) {
                e.printStackTrace();
                try {
                    client.sendRequest();
                } catch (IOException e1) {
                    e1.printStackTrace();
                }
            }

            // 每隔 2 秒中发送一次请求到服务端
            Thread.sleep(2000);
        }
    }
}