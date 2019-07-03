package com.rock.socket;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
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


        SocketSource source1 = new SocketSource("localhost", 9000, "\n", "UTF-8");
        //SocketSource source2 = new SocketSource("localhost", 8000, "\n", "UTF-8");

        tableEnv.registerDataStream("call_record", getStream(env, source1), "phone,call,call_time.rowtime");

        //查询5分钟内每个手机号的呼叫次数
        String sql = "select phone,count(1) from call_record group by tumble(call_time,INTERVAL '5' SECOND), phone";
        //


        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        /*DataStreamSink<Row> rowDataStreamSink = dataStream.addSink(new MySqlSink());*/

        env.execute("SqlSocketToMysql");

    }

    private static DataStream<Row> getStream(StreamExecutionEnvironment env, SocketSource socketSource) {
        DataStreamSource<String> stringDataStreamSource = env.addSource(socketSource);

        TypeInformation[] typeInformations = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};

        DataStream<Row> map = stringDataStreamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) {
                String[] split = s.split(",");
                Row row = new Row(split.length);
                row.setField(0, split[0]);
                row.setField(1, split[1]);
                row.setField(2, Long.valueOf(split[2]));
                return row;
            }
        }).returns(new RowTypeInfo(typeInformations));

        // 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return map.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
            private long currentMaxTimestamp = 0L;

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp) {
                long field = (long) element.getField(2);
                try {
                    currentMaxTimestamp = field;
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return currentMaxTimestamp;
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp);
            }
        });
    }

    static class MySqlSink extends RichSinkFunction<Row> {
        private static final long serialVersionUID = 1L;
        private Connection conn = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            Class.forName("com.mysql.jdbc.Driver");
            // 获取数据库连接
            conn = DriverManager.getConnection("jdbc:mysql://39.107.224.153:3306/kangkang", "root", "wodeai1993");
        }

        @Override
        public void invoke(Row value, Context context) throws Exception {
            PreparedStatement ps = null;
            try {
                ps = conn.prepareStatement("insert into test (sex,count) values (?,?)");
                ps.setString(1, (String) value.getField(0));
                ps.setInt(2, Integer.valueOf(String.valueOf(value.getField(1))));
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
