package com.rock.async;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase.KEY_DISABLE_METRICS;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class AsyncTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.getTableEnvironment(env);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        DataStream<String> source = env.readTextFile("hdfs://172.16.44.28:8020/flink/userClick_Random_1Y", "UTF-8");

        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};
        String[] fields = new String[]{"id", "user_click", "time"};
        RowTypeInfo typeInformation = new RowTypeInfo(types, fields);

        DataStream<Row> stream = source.map(new MapFunction<String, Row>() {
            private static final long serialVersionUID = 2349572544179673349L;

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
                    row.setField(i, value);
                }
                return row;
            }
        });
        //异步io拼接字段
        DataStream<Row> orderedWait = AsyncDataStream.orderedWait(stream, new AsyncFunction(), 10000, TimeUnit.MILLISECONDS, 1000);
        //拼接字段后新的列信息
        TypeInformation[] joinTypes = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG, Types.STRING};
        String[] joinFields = new String[]{"id", "user_click", "time", "name"};
        RowTypeInfo joinTypeInfo = new RowTypeInfo(joinTypes, joinFields);
        //设置新的列信息
        orderedWait.getTransformation().setOutputType(joinTypeInfo);

        //注册表
        tableEnv.registerDataStream("user_click_name", orderedWait, String.join(",", joinTypeInfo.getFieldNames()));

        String sql1 = "select * from user_click_name";

        Table tableQuery = tableEnv.sqlQuery(sql1);

        DataStream<Row> result = tableEnv.toAppendStream(tableQuery, Row.class);


        result.print();

        /*SingleOutputStreamOperator<String> map = result.map((MapFunction<Row, String>) row -> {
            Object userId = row.getField(0);
            Object userClick = row.getField(1);
            Object userName = row.getField(3);
            return userId + "," + userClick + "," + userName;
        });
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.44.28:9099");
        properties.setProperty("retries", "3");
        properties.setProperty(KEY_DISABLE_METRICS, "true");
        map.addSink(new FlinkKafkaProducer011<>("side_table_test", new SimpleStringSchema(StandardCharsets.UTF_8), properties));*/

        env.execute("AsyncTest");
    }

    /**
     * 暂时用不到
     *
     * @param stream
     * @param env
     * @param tableEnv
     * @param adaptSql
     * @return org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.types.Row>
     * @author cuishilei
     * @date 2019/8/16
     */
    private DataStream<Row> registerUdf(DataStream<Row> stream, StreamExecutionEnvironment env, StreamTableEnvironment tableEnv, String adaptSql) {
        //注册 adapt 表，先执行 udf 操作
        Table table = tableEnv.fromDataStream(stream, "id,user_click,time");
        tableEnv.registerTable("user_click_adapt", table);
        Table tableAdapt = tableEnv.sqlQuery("");

        RowTypeInfo typeInfo = new RowTypeInfo(table.getSchema().getFieldTypes(), table.getSchema().getFieldNames());
        String fieldsStr = String.join(",", typeInfo.getFieldNames());
        DataStream adaptStream = tableEnv.toAppendStream(table, typeInfo);
        Table regTable = tableEnv.fromDataStream(adaptStream, fieldsStr);
        tableEnv.registerTable("user_click", regTable);
        return tableEnv.toAppendStream(regTable, Row.class);
    }
}
