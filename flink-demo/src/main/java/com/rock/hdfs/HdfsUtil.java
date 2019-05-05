package com.rock.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author RockeyCui
 */
public class HdfsUtil {
    private static final String FILE_SUFFIX = ".metadata";

    private static FileSystem hdfs;


    /**
     * @return 得到hdfs的连接 FileSystem类
     * @throws URISyntaxException
     * @throws IOException
     */
    static {
        try {
            Configuration config = new Configuration();
            URI uri = new URI("hdfs://localhost:9000");
            hdfs = FileSystem.get(uri, config);
        } catch (Exception e) {
            throw new RuntimeException("HDFS init error");
        }
    }

    /**
     * 检查文件或者文件夹是否存在
     *
     * @param filename
     * @return
     */
    public static boolean checkFileExist(String filename) {
        try {
            Path f = new Path(filename + FILE_SUFFIX);
            return hdfs.exists(f);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return false;
    }

    /**
     * 将一个字符串写入某个路径
     *
     * @param text 要保存的字符串
     * @param path 要保存的路径
     */
    public static void writerString(String text, String path) {
        FSDataOutputStream os = null;
        try {
            Path f = new Path(path);
            os = hdfs.create(f, true);
            // 以UTF-8格式写入文件，不乱码
            os.writeUTF(text);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(os);
        }
    }

    /**
     * 将一个字符串写入某个文件
     *
     * @param path 要保存的路径
     */
    public static void writerJson(String json, String path) {
        FSDataOutputStream os = null;
        try {
            Path f = new Path(path);
            os = hdfs.create(f, true);
            // 以UTF-8格式写入文件，不乱码
            os.writeUTF(json);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(os);
        }
    }

    public static String readString(String path) {
        FSDataInputStream is = null;
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        try {
            Path f = new Path(path);
            is = hdfs.open(f);
            IOUtils.copyBytes(is, os, 1024);
            return is.readUTF();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(is);
        }
        return null;
    }

    public static void main(String[] args) {
        //HdfsUtil.writerString("cuishilei", "/user/rock/test");
        String s = HdfsUtil.readString("/user/rock/test");
        System.out.println(s);
    }
}
