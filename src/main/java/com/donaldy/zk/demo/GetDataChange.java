package com.donaldy.zk.demo;

import org.I0Itec.zkclient.IZkDataListener;
import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;

/**
 * 使用监听器监听节点数据的变化
 *
 * @author donald
 * @date 2020/08/27
 */
public class GetDataChange {

    public static void main(String[] args) throws InterruptedException {

        // 获取zkClient对象
        final ZkClient zkClient = new ZkClient("172.16.64.121:2181");

        // 设置自定义的序列化类型,否则会报错!!
        zkClient.setZkSerializer(new ZkStrSerializer());

        // 判断节点是否存在,不存在创建节点并赋值
        final boolean exists = zkClient.exists("/data-client");

        if (!exists) {
            zkClient.createEphemeral("/data-client", "123");
        }

        // 注册监听器,节点数据改变的类型,接收通知后的处理逻辑定义
        zkClient.subscribeDataChanges("/data-client", new IZkDataListener() {
            public void handleDataChange(String path, Object data) throws Exception {

                // 定义接收通知之后的处理逻辑
                System.out.println(path + " data is changed, new data " + data);
            }

            // 数据删除--》节点删除
            public void handleDataDeleted(String path) throws Exception {

                System.out.println(path + " is deleted!!");
            }
        });

        // 更新节点的数据,删除节点,验证监听器是否正常运行
        final Object o = zkClient.readData("/data-client");

        System.out.println(o);

        zkClient.writeData("/data-client", "new data");
        Thread.sleep(1000);

        // 删除节点
        zkClient.delete("/data-client");
        Thread.sleep(2000);
    }
}

class ZkStrSerializer implements ZkSerializer {

    //序列列化,数据--》byte[]
    public byte[] serialize(Object o) throws ZkMarshallingError {
        return String.valueOf(o).getBytes();
    }

    //反序列列化,byte[]--->数据
    public Object deserialize(byte[] bytes) throws ZkMarshallingError {
        return new String(bytes);
    }
}