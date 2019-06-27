package com.rock.socket;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class SqlSocketToMysql {
	public static void main(String[] args) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

		SocketSource source = new SocketSource("localhost", 9000, "\n", "UTF-8");

		DataStreamSource<String> stringDataStreamSource = env.addSource(source);

		TypeInformation[] typeInformations = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};

		DataStream<Row> map = stringDataStreamSource.map(new MapFunction<String, Row>() {
			@Override
			public Row map(String s) {
				String[] split = s.split(",");
				Row row = new Row(split.length + 1);
				for (int i = 0; i < split.length; i++) {
					row.setField(i, split[i]);
				}
				row.setField(split.length, System.currentTimeMillis());
				return row;
			}
		}).returns(new RowTypeInfo(typeInformations));

		tableEnv.registerDataStream("user1", map, "name,sex");

		Table table = tableEnv.sqlQuery("select * from user1 where sex='1'");

		DataStream<Row> dataStream = tableEnv.toAppendStream(table, Row.class);


		DataStreamSink<Row> rowDataStreamSink = dataStream.addSink(new MySqlSink());

		env.execute("SqlSocketToMysql");

	}

	static class MySqlSink extends RichSinkFunction<Row> {
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
		public void invoke(Row value, Context context) throws Exception {
			PreparedStatement ps = null;
			try {
				ps = conn.prepareStatement("insert into test (name,sex) values (?,?)");
				ps.setString(1, (String) value.getField(0));
				ps.setString(2, (String) value.getField(1));
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

}
