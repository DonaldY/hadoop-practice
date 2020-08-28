package com.donaldy.zk.lock;

/**
 * zk实现分布式锁
 *
 * @author donald
 * @date 2020/08/27
 */
public class DisLockTest {
    public static void main(String[] args) {

        // 使用10个线程模拟分布式环境
        for (int i = 0; i < 10; i++) {
            new Thread(new DisLockRunnable()).start();//启动线程
        }
    }

    static class DisLockRunnable implements Runnable {

        public void run() {

            final DisClient client = new DisClient();

            client.getDisLock();

            try {
                Thread.sleep(2000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }

            client.deleteLock();
        }
    }
}