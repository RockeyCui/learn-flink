package com.rock.gendata;

import org.apache.commons.cli.*;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.*;

public class GenData {
    private static Random random = new Random();

    private static String[] telFirst = "134,135,136,137,138,139,150,151,152,157,158,159,130,131,132,155,156,133,153".split(",");
    private static String[] baseStation = {"124,224", "125,225", "126,226", "127,227"};


    private static String getTel() {
        int index = getNum(0, telFirst.length - 1);
        String first = telFirst[index];
        String second = String.valueOf(getNum(1, 888) + 10000).substring(1);
        String third = String.valueOf(getNum(1, 9100) + 10000).substring(1);
        return first + second + third;
    }

    private static int getNum(int start, int end) {
        return (int) (Math.random() * (end - start + 1) + start);
    }

    private static String getBaseStation() {
        //return baseStation[random.nextInt(baseStation.length)];
        return "124,224";
    }

    private static void writeNIO(String data) {

    }


    public static void main(String[] args) throws IOException, InterruptedException {
        String userDir = System.getProperty("user.dir");

        CommandLine help = getHelp(args);
        if (help != null) {
            Option[] options = help.getOptions();
            String name = help.getOptionValue("name");
            String path = help.getOptionValue("path", System.getProperty("user.dir"));
            String spoolDir = help.getOptionValue("spoolDir");
            String phone = help.getOptionValue("phone");
            String times = help.getOptionValue("times");
            String filePath = path + File.separator + name;
            System.out.println("file->" + filePath);
            System.out.println("phone->" + phone);
            System.out.println("times->" + times);

            System.out.println("Do you want to continue? yes/no");
            Scanner scanner = new Scanner(System.in);
            String s = scanner.next();
            if ("yes".equals(s)) {
                gen(phone, times, filePath);
            } else {
                System.out.println("exit......");
            }
        } else {
            System.out.println("exit......");
        }
    }

    private static void gen(String phone, String times, String filePath) throws IOException, InterruptedException {
        //这个作为命中基站的手机号但前十分钟跳出了
        List<String> list1 = new ArrayList<>();
        list1.add("10000000000");
        //这个作为命中基站的手机号
        List<String> list2 = new ArrayList<>();
        list2.add("20000000000");
        list2.add("30000000000");

        Set<String> phones = new HashSet<>();
        phones.addAll(list1);
        phones.addAll(list2);
        //这个作为随机的手机号
        do {
            phones.add(getTel());
        } while (phones.size() < Integer.valueOf(phone));

        System.out.println("phone size " + phone);
        File file = new File(filePath);
        if (!file.exists()) {
            boolean newFile = file.createNewFile();
            if (!newFile) {
                throw new RuntimeException("create file error");
            }
        }
        // long time = System.currentTimeMillis();
        for (int i = 0; i < Integer.valueOf(times); i++) {
            Thread.sleep(1000);
            long time = System.currentTimeMillis();
            Date timestampToDate = DateTimeUtil.getTimestampToDate(time);
            //Date timestampToDate = DateTimeUtil.getTimestampToDate(time + i * 1000);
            String dateTimeToString = DateTimeUtil.getDateTimeToString(timestampToDate, DateTimeUtil.DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS);
            List<String> lines = new ArrayList<>();
            for (String one : phones) {
                if (list1.contains(one)) {
                    if (i == 50) {
                        //在第50秒跑出基站
                        lines.add(one + "," + getBaseStation() + "," + dateTimeToString);
                    } else {
                        lines.add(one + "," + "123,456" + "," + dateTimeToString);
                    }
                } else if (list2.contains(one)) {
                    lines.add(one + "," + "123,456" + "," + dateTimeToString);
                } else {
                    lines.add(one + "," + getBaseStation() + "," + dateTimeToString);
                }
            }
            Files.write(file.toPath(), lines, StandardCharsets.UTF_8, StandardOpenOption.APPEND);
            if (i % 60 == 0) {
                System.out.println("=====>" + dateTimeToString);
            } else {
                System.out.println(dateTimeToString);
            }
        }
    }


    private static CommandLine getHelp(String[] args) {
        Option[] opts = null;

        HelpFormatter hf = new HelpFormatter();
        hf.setWidth(110);
        CommandLine commandLine = null;
        CommandLineParser parser = new DefaultParser();

        Options options = ArgsOption.getOption();
        try {
            commandLine = parser.parse(options, args);
            if (commandLine.getOptions().length == 0) {
                hf.printHelp("this is help", options, true);
            } else {
                if (commandLine.hasOption("help")) {
                    // 打印使用帮助
                    hf.printHelp("", options, true);
                }
            }
        } catch (ParseException e) {
            hf.printHelp("this is help", options, true);
        }
        return commandLine;
    }
}
