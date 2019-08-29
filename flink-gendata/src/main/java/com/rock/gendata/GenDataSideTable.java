package com.rock.gendata;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

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
        gen(25000000, "D:\\userClick_Random_2500W");
    }

    private static void gen(int times, String filePath) throws IOException {
        File file = new File(filePath);
        if (!file.exists()) {
            boolean newFile = file.createNewFile();
            if (!newFile) {
                throw new RuntimeException("create file error");
            }
        }
        List<String> lines = new ArrayList<>();
        for (int i = 0; i < times; i++) {
            long time = System.currentTimeMillis();
            int userId = getUserId();
            String line = userId + "," + "/good/" + i + "," + time;
            lines.add(line);
            if (lines.size() == 10000) {
                Files.write(file.toPath(), lines, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
                lines.clear();
            }
        }
    }

    private static int getUserId() {
        int index = random.nextInt(userIds.size() - 1);
        return userIds.get(index);
    }
}
