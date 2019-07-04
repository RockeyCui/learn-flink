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

        tableEnv.registerDataStream("call_record",
                TestUtil.getStream(env, source1, types),
                "phone,call,call_time.rowtime");

        //注册 udf 换算时间时区 因为 flink 默认时区为标准时区
        tableEnv.registerFunction("utc2local", new UTC2Local());

        //注册 udf 计算两个 timestamp 差值
        tableEnv.registerFunction("TimeStampCompare", new TimeStampCompare());

        //统计每5秒钟内每个手机号的呼叫次数
        String sql = "select" +
                " phone," +
                " count(1)," +
                " utc2local(TUMBLE_START(call_time, INTERVAL '5' SECOND)) as wStart," +
                " utc2local(TUMBLE_END(call_time, INTERVAL '5' SECOND)) as wEnd" +
                " from call_record" +
                " group by TUMBLE(call_time,INTERVAL '5' SECOND), phone";

        //统计每5秒钟内手机号为 150 的呼叫次数
        String sql1 = "select" +
                " phone," +
                " count(1)," +
                " utc2local(TUMBLE_START(call_time, INTERVAL '5' SECOND)) as wStart," +
                " utc2local(TUMBLE_END(call_time, INTERVAL '5' SECOND)) as wEnd" +
                " from call_record" +
                " where phone = '150'" +
                " group by TUMBLE(call_time,INTERVAL '5' SECOND), phone";

        //测试而已，没什么实际含义
        String sql2 = "select" +
                " phone," +
                //" min(call_time) min_time," +
                //" max(call_time) max_time," +
                //" TimeStampCompare(max(call_time),min(call_time)) bbb," +
                " utc2local(HOP_START(call_time,INTERVAL '1' SECOND,INTERVAL '10' SECOND)) as wStart," +
                " utc2local(HOP_END(call_time,INTERVAL '1' SECOND,INTERVAL '10' SECOND)) as wEnd" +
                " from call_record" +
                " group by HOP(call_time,INTERVAL '1' SECOND,INTERVAL '10' SECOND), phone" +
                " having TimeStampCompare(max(call_time),min(call_time)) >= 9000" +
                "";

        Table table = tableEnv.sqlQuery(sql2);

        DataStream<Row> rowDataStream = tableEnv.toAppendStream(table, Row.class);

        rowDataStream.print();

        env.execute("SocketByTableSql");
    }
}
