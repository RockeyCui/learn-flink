package com.rock.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author RockeyCui
 */
public class MySqlSink extends RichSinkFunction<Tuple2<Boolean, Row>> {

    private static final long serialVersionUID = 1L;
    private Connection conn = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        // 获取数据库连接
        conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/rock", "root", "root");
    }

    @Override
    public void invoke(Tuple2<Boolean, Row> value, Context context) throws Exception {
        PreparedStatement ps = null;
        try {
            Row row = value.f1;
            ps = conn.prepareStatement("insert into test (name,count) values (?,?)");
            ps.setString(1, String.valueOf(row.getField(0)));
            ps.setInt(2, Math.toIntExact((Long) row.getField(1)));
            ps.executeUpdate();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (ps != null) {
                ps.close();
            }
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
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
