package com.rock.socket;

import com.rock.util.TimeStampCompare;
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

        env.setParallelism(1);

        SocketSource source1 = new SocketSource("localhost", 9000, "\n", "UTF-8");
        //SocketSource source2 = new SocketSource("localhost", 8000, "\n", "UTF-8");


        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};

        //注册为表
        //phone 电话
        //called 被呼叫人
        //call_time 呼叫时刻
        tableEnv.registerDataStream("call_record",
                TestUtil.getStream(env, source1, types),
                "phone,called,call_time.rowtime");

        //注册 udf 换算时间时区 因为 flink 默认时区为标准时区
        tableEnv.registerFunction("utc2local", new UTC2Local());

        //注册 udf 计算两个 timestamp 差值
        tableEnv.registerFunction("TimeStampCompare", new TimeStampCompare());

        //统计每5秒钟内每个手机号的呼叫次数，这里的5s不是说程序等待5秒，而是数据流里的呼叫时间差达到5s
        String sql = "select" +
                " phone," +
                " count(1)," +
                " utc2local(TUMBLE_START(call_time, INTERVAL '5' SECOND)) as wStart," +
                " utc2local(TUMBLE_END(call_time, INTERVAL '5' SECOND)) as wEnd" +
                " from call_record" +
                " group by TUMBLE(call_time,INTERVAL '5' SECOND), phone";

        //统计每5秒钟内手机号为 150 的呼叫次数，这里的5s不是说程序等待5秒，而是数据流里的呼叫时间差达到5s
        String sql1 = "select" +
                " phone," +
                " count(1)," +
                " utc2local(TUMBLE_START(call_time, INTERVAL '5' SECOND)) as wStart," +
                " utc2local(TUMBLE_END(call_time, INTERVAL '5' SECOND)) as wEnd" +
                " from call_record" +
                " where phone = '150'" +
                " group by TUMBLE(call_time,INTERVAL '5' SECOND), phone";

        //每 1s 统计给 123 呼叫间隔超过 10s 的手机号
        String sql2 = "select" +
                " phone" +
                " from call_record" +
                " group by HOP(call_time,INTERVAL '1' SECOND,INTERVAL '10' SECOND), phone" +
                " having  max(call_time) - INTERVAL '10' SECOND + INTERVAL '1' SECOND >= min(call_time)" +
                "";


        //每 1s 统计前 10s 一直（期间没给别的打过电话）在呼叫 123的手机号
        String sql3 = "select" +
                " phone" +
                " from call_record" +
                " group by HOP(call_time,INTERVAL '1' SECOND,INTERVAL '10' SECOND), phone" +
                " having" +
                " max(call_time) - INTERVAL '10' SECOND + INTERVAL '1' SECOND >= min(call_time)" +
                " and" +
                " COUNT(1) = SUM(CASE WHEN called = '123' THEN 1 ELSE 0 END)" +
                "";

        //统计每5秒钟内的呼叫次数超过 100 的手机号（骚扰电话监控），这里的5s不是说程序等待5秒，而是数据流里的呼叫时间差达到5s
        String sql4 = "select" +
                " phone," +
                " count(1)," +
                " utc2local(TUMBLE_START(call_time, INTERVAL '5' SECOND)) as wStart," +
                " utc2local(TUMBLE_END(call_time, INTERVAL '5' SECOND)) as wEnd" +
                " from call_record" +
                " group by TUMBLE(call_time,INTERVAL '5' SECOND), phone" +
                " having" +
                " COUNT(1) >= 100";

        Table table = tableEnv.sqlQuery(sql4);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute("SocketByTableSql");
    }
}
