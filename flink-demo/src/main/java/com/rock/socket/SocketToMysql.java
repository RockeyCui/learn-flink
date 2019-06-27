package com.rock.socket;

import com.rock.mysql.MySqlSink;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class SocketToMysql {
	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


		DataStream<String> text = env.socketTextStream("localhost", 9000, "\n");

		DataStream<Tuple2<String, Integer>> reduce = text
				.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
					@Override
					public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
						collector.collect(new Tuple2<>(s, 1));
					}
				}).keyBy(0)
				.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
					@Override
					public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) {
						return new Tuple2<>(value1.f0, value1.f1 + value2.f1);
					}
				});

		reduce.addSink(new MySqlSink());
		env.execute("SocketToMysql");
	}
}
