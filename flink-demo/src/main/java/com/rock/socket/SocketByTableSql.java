package com.rock.socket;

import com.rock.util.UTC2Local;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class SocketByTableSql {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);


        SocketSource source1 = new SocketSource("localhost", 9000, "\n", "UTF-8");
        //SocketSource source2 = new SocketSource("localhost", 8000, "\n", "UTF-8");


        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};

        tableEnv.registerDataStream("call_record",
                TestUtil.getStream(env, source1, types),
                "phone,call,call_time.rowtime");
        //注册
        tableEnv.registerFunction("utc2local", new UTC2Local());
        //查询5秒钟内每个手机号的呼叫次数
        String sql = "select" +
                " phone," +
                " count(1)," +
                " utc2local(TUMBLE_START(call_time, INTERVAL '5' SECOND)) as wStart," +
                " utc2local(TUMBLE_END(call_time, INTERVAL '5' SECOND)) as wEnd" +
                " from call_record" +
                " group by tumble(call_time,INTERVAL '5' SECOND), phone";
        //查询5秒钟内手机号为 150 的呼叫次数
        String sql1 = "select phone,count(1) from call_record group by tumble(call_time,INTERVAL '5' SECOND), phone where phone = '150'";

        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        /*DataStreamSink<Row> rowDataStreamSink = dataStream.addSink(new MySqlSink());*/

        env.execute("SocketByTableSql");

    }
}
