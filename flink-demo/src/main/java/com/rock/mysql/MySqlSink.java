package com.rock.mysql;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * @author cuishilei
 * @date 2019/6/25
 */
public class MySqlSink extends RichSinkFunction<Tuple2<String, Integer>> {
	private static final long serialVersionUID = 1L;
	private Connection conn = null;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		super.open(parameters);
		Class.forName("com.mysql.jdbc.Driver");
		// 获取数据库连接
		conn = DriverManager.getConnection("jdbc:mysql://39.107.224.153:3306/kangkang", "root", "wodeai1993");
	}

	@Override
	public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement("insert into test (name,count) values (?,?)");
			ps.setString(1, value.f0);
			ps.setInt(2, value.f1);
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
