package com.rock.socket;

import com.rock.myutil.DateTimeUtil;

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
        DATA.add("150,124,");
        TIME = 1562730986000L;
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
                String data = DATA.get(0) + (TIME + 1000 * i);
                if (i == 8) {
                    data = DATA.get(1) + (TIME + 1000 * i);
                }
                System.out.println("send:" + data + "  " + DateTimeUtil.getTimestampToDate(Long.parseLong(data.split(",")[2])));
                outputStream.write((data + "\n").getBytes(Charset.forName("UTF-8")));
                outputStream.flush();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static String getData(int i) {
        //random.nextInt(DATA.size())
        return DATA.get(0) + (TIME + 1000 * i);
    }
}
