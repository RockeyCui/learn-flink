package com.rock.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @author RockeyCui
 */
public class MySqlRich extends RichSourceFunction<Tuple2<String, String>> {
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        // 获取数据库连接
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/rock", "root", "root");

    }

    @Override
    public void run(SourceContext<Tuple2<String, String>> sourceContext) throws Exception {
        PreparedStatement ps = null;
        try {
            ps = conn.prepareStatement("select * from test");
            ResultSet resultSet = ps.executeQuery();
            while (resultSet.next()) {
                String name = resultSet.getString("name");
                String id = resultSet.getString("count");
                Tuple2<String, String> tuple2 = new Tuple2<>();
                tuple2.setFields(id, name);
                //发送结果，结果是tuple2类型，2表示两个元素，可根据实际情况选择
                sourceContext.collect(tuple2);
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    @Override
    public void cancel() {
        try {
            super.close();
            if (conn != null) {
                conn.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
