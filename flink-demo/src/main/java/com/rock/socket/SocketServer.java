package com.rock.socket;

import com.rock.util.DateTimeUtil;

import java.io.DataOutputStream;
import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class SocketServer {
    private static Random random = new Random();

    private static final int PORT = 9000;

    private static List<String> DATA = new ArrayList<>();

    private static long TIME;

    static {
        DATA.add("150,123,");
        DATA.add("155,123,");
        DATA.add("159,123,");
        TIME = System.currentTimeMillis();
    }

    public static void main(String[] args) {
        send();
    }


    private static void send() {
        ServerSocket server;
        Socket socket;
        DataOutputStream out;
        try {
            server = new ServerSocket(PORT);
            socket = server.accept();
            OutputStream outputStream = socket.getOutputStream();
            for (int i = 0; i < 100; i++) {
                Thread.sleep(1000);
                String data = getData(i);
                System.out.println("send:" + data + "  " + DateTimeUtil.getTimestampToDate(Long.parseLong(data.split(",")[2])));
                outputStream.write((data + "\n").getBytes(Charset.forName("UTF-8")));
                outputStream.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getData(int i) {
        return DATA.get(random.nextInt(DATA.size())) + (TIME + 1000 * i);
    }
}
