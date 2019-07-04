package com.rock.socket;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class SocketByApi {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);

        SocketSource source = new SocketSource("localhost", 9000, "\n", "UTF-8");
        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};
        DataStream<Row> stream = TestUtil.getStream(env, source, types);

        SingleOutputStreamOperator<Object> apply = stream.keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5))).apply(new WindowFunction<Row, Object, Tuple, TimeWindow>() {
            @Override
            public void apply(Tuple tuple, TimeWindow timeWindow, Iterable<Row> iterable, Collector<Object> collector) throws Exception {
                String key = tuple.toString();
                List<String> list = new ArrayList<>();
                Iterator<Row> it = iterable.iterator();
                while (it.hasNext()) {
                    Row next = it.next();
                    list.add(String.valueOf(next.getField(0)));
                }
                Collections.sort(list);
                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                String result = key + "," + list.size() + "," + sdf.format(timeWindow.getStart()) + "," + sdf.format(timeWindow.getEnd());
                collector.collect(result);
            }
        });
        apply.print();

        env.execute("SocketByApi");
    }
}
