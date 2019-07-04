package com.rock.socket;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.types.Row;

public class TestUtil {

    /**
     * 获取添加水印的 stream
     * 数据格式  f1,f2,...,timestamp
     *
     * @param env          执行环境
     * @param sourceFunction 数据源
     * @param types        每个数据的类型
     * @return
     */
    public static DataStream<Row> getStream(StreamExecutionEnvironment env, SourceFunction sourceFunction, TypeInformation[] types) {
        DataStreamSource<String> stringDataStreamSource = env.addSource(sourceFunction);

        DataStream<Row> map = stringDataStreamSource.map(new MapFunction<String, Row>() {
            @Override
            public Row map(String s) {
                String[] split = s.split(",");
                Row row = new Row(split.length);
                for (int i = 0; i < split.length; i++) {
                    Object value = split[i];
                    if (types[i].equals(Types.STRING)) {
                        value = split[i];
                    }
                    if (types[i].equals(Types.LONG)) {
                        value = Long.valueOf(split[i]);
                    }
                    if (types[i].equals(Types.INT)) {
                        value = Integer.valueOf(split[i]);
                    }
                    row.setField(i, value);
                }
                return row;
            }
        }).returns(new RowTypeInfo(types));

        // 事件时间
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        return map.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<Row>() {
            private long currentMaxTimestamp = 0L;

            @Override
            public long extractTimestamp(Row element, long previousElementTimestamp) {

                long eventTime = (long) element.getField(2);
                try {
                    currentMaxTimestamp = Math.max(eventTime, currentMaxTimestamp);
                } catch (Exception e) {
                    e.printStackTrace();
                }
                return eventTime;
            }

            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp);
            }
        });
    }
}
