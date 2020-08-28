package com.donaldy.zk.watch;


import java.io.IOException;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Date;

/**
 * @author donald
 * @date 2020/08/27
 */

public class TimeServer extends Thread {

    private int port = 0;

    TimeServer(int port) {
        this.port = port;
    }

    @Override
    public void run() {

        //启动 server socket监听一个端口
        try {
            final ServerSocket serverSocket = new ServerSocket(port);

            while(true){
                final Socket socket = serverSocket.accept();
                final OutputStream out = socket.getOutputStream();
                out.write(new Date().toString().getBytes());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}