package com.rock.watermark;

import com.rock.myutil.DateTimeUtil;
import com.rock.socket.SocketSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import javax.annotation.Nullable;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author cuishilei
 * @date 2019/7/31
 */
public class Test {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //数据格式
        //001 -> 1,1,2019-07-01 00:00:01
        //002 -> 1,2,2019-07-01 00:00:02
        //003 -> 1,3,2019-07-01 00:00:03
        //004 -> 1,4,2019-07-01 00:00:04
        //005 -> 1,5,2019-07-01 00:00:05
        //006 -> 1,6,2019-07-01 00:00:06
        //007 -> 1,3,2019-07-01 00:00:03 -->迟到数据
        //008 -> 1,7,2019-07-01 00:00:07
        //009 -> 1,8,2019-07-01 00:00:08
        //010 -> 1,9,2019-07-01 00:00:09
        //011 -> 1,3,2019-07-01 00:00:03 -->迟到数据
        //012 -> 1,10,2019-07-01 00:00:10
        //013 -> 1,11,2019-07-01 00:00:11
        //014 -> 1,12,2019-07-01 00:00:12
        //015 -> 1,13,2019-07-01 00:00:13
        //016 -> 1,14,2019-07-01 00:00:14

        //窗口 5s 窗口起始设定flink是根据窗口的倍数设置的
        // 比如 5s 窗口，流中第一条数据为 2019-07-01 00:00:01 ==> 1561910401000 则起始时间为 1561910400000 为 5000 的倍数
        //水位线 2s
        //窗口1 00-05
        //窗口2 05-10
        //窗口3 10-15
        //窗口包左不包右
        //窗的触发时机 watermark >= window_end_time
        //01 watermark 59 window_end_time 05
        //02 watermark 00 window_end_time 05
        //....
        //07 watermark 05 window_end_time 04 ==> 触发
        //
        //第一个窗口 1+2+3+4+3=13
        //第二个窗口 5+6+7+8+9=35
        //第三个窗口 10+11+12+13+14=60


        SingleOutputStreamOperator<MyEvent> reduce = getMyEventFromSocket(env)
                .keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce((ReduceFunction<MyEvent>) (t1, t2) -> new MyEvent(t1.getId(), t1.getNum() + t2.getNum(), t2.getEventTime()));
        reduce.print();

       /* DataStream<String> window = getMyEventFromSocket(env)
                .keyBy(MyEvent::getId)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new WindowFunction<MyEvent, String, Integer, TimeWindow>() {
                    @Override
                    public void apply(Integer integer, TimeWindow timeWindow, Iterable<MyEvent> iterable, Collector<String> collector) throws Exception {
                        Integer key = integer;
                        List<MyEvent> list = new ArrayList<>();
                        for (MyEvent next : iterable) {
                            list.add(next);
                        }
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
                        String result = key + "," + list.size() + "," + sdf.format(timeWindow.getStart()) + "," + sdf.format(timeWindow.getEnd());
                        collector.collect(result);
                    }
                });
        window.print();*/

        env.execute("fun");
    }

    private static DataStream<MyEvent> getMyEventFromSocket(StreamExecutionEnvironment env) {
        SocketSource socketSource = new SocketSource("localhost", 9000, "\n", "UTF-8");
        DataStreamSource<String> stringDataStreamSource = env.addSource(socketSource);
        return stringDataStreamSource.map(new MapFunction<String, MyEvent>() {
            @Override
            public MyEvent map(String s) {
                String[] split = s.split(",");
                int key = Integer.parseInt(split[0]);
                int num = Integer.parseInt(split[1]);
                String time = split[2];
                return new MyEvent(key, num, time);
            }
        }).assignTimestampsAndWatermarks(new MyWatermarks());
    }

    static class MyWatermarks implements AssignerWithPeriodicWatermarks<MyEvent> {
        private long currentMaxTimestamp;

        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

        @Nullable
        @Override
        public Watermark getCurrentWatermark() {
            long offset = 2000L;
            return new Watermark(currentMaxTimestamp - offset);
        }

        @Override
        public long extractTimestamp(MyEvent myEvent, long l) {
            long eventTime;
            try {
                Date stringToDateTime = DateTimeUtil.getStringToDateTime(myEvent.getEventTime(), DateTimeUtil.DATETIME_FORMAT_YYYY_MM_DD_HH_MM_SS);
                eventTime = stringToDateTime.getTime();
                currentMaxTimestamp = Math.max(eventTime, currentMaxTimestamp);
                System.out.println("eventTime:[ " + sdf.format(eventTime) + " ]," +
                        "Watermark:[ " + sdf.format(getCurrentWatermark().getTimestamp()) + " ]" +
                        "currentMaxTimestamp:[ " + sdf.format(currentMaxTimestamp) + " ],");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return eventTime;
        }
    }
}
