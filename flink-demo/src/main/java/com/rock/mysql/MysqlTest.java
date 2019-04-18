package com.rock.mysql;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author RockeyCui
 */
public class MysqlTest {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Tuple2<String, String>> dataStreamSource = env.addSource(new MySqlRich());

        DataStream<Tuple2<String, String>> returnDataStream = dataStreamSource
                .flatMap(new FlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {
                    @Override
                    public void flatMap(Tuple2<String, String> stringStringTuple2, Collector<Tuple2<String, String>> collector) throws Exception {
                        collector.collect(stringStringTuple2);
                    }
                });
        // print the results with a single thread, rather than in parallel
        returnDataStream.print().setParallelism(1);

        env.execute("Mysql Test");
    }
}
