package com.rock.gendata;

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class GenDataSideTable {
    private static Random random = new Random();
    private static List<Integer> userIds = new ArrayList<>();

    static {
        for (int i = 0; i < 10000; i++) {
            int userId = 1000 + i;
            userIds.add(userId);
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        String userDir = System.getProperty("user.dir");
        gen(100000, "D:\\userClick_Random_1Y");
    }

    private static void gen(int times, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            boolean newFile = file.createNewFile();
            if (!newFile) {
                throw new RuntimeException("create file error");
            }
        }
        for (int i = 0; i < times; i++) {
            List<String> lines = new ArrayList<>();
            long time = System.currentTimeMillis();
            for (int j = 0; j < 1000; j++) {
                int userId = getUserId();
                String userClick = "/good/" + i + "/" + j;
                String line = userId + "," + userClick + "," + time;
                lines.add(line);
            }
            Files.write(file.toPath(), lines, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        }
    }

    private static int getUserId() {
        int index = random.nextInt(userIds.size() - 1);
        return userIds.get(index);
    }
}
