package com.rock.flink19.lookup;

import com.rock.flink19.lookup.tablesource.RedisLookupTableSource;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import java.nio.charset.StandardCharsets;
import java.util.Properties;

import static org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumerBase.KEY_DISABLE_METRICS;

/**
 * @author cuishilei
 * @date 2019/9/1
 */
public class LookUpTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, settings);

        DataStream<String> source = env.readTextFile("D:\\userClick_Random_100W", "UTF-8");

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
        }).returns(typeInformation);

        tableEnv.registerDataStream("user_click_name", stream, String.join(",", typeInformation.getFieldNames()) + ",proctime.proctime");

        RedisLookupTableSource tableSource = getTableSource(true, false, "w", 10000, 30000);

        tableEnv.registerTableSource("info", tableSource);

        String sql = "select a.id, a.user_click, i.userName, i.userSex" +
                " from" +
                " user_click_name as a" +
                " join info FOR SYSTEM_TIME AS OF a.proctime as i on i.userId = a.id";

        Table table = tableEnv.sqlQuery(sql);

        DataStream<Row> result = tableEnv.toAppendStream(table, Row.class);

        result.print().setParallelism(1);

        //sendToKafka(result);

        tableEnv.execute("LookUpTest");
    }

    private static RedisLookupTableSource getTableSource(boolean isAsync, boolean isCached, String cacheType, long cacheMaxSize, long cacheExpireMs) {
        return RedisLookupTableSource.newBuilder()
                .withIp("172.16.44.28")
                .withPort(6379)
                .withDatabase("0")
                .withIsCached(isCached)
                .withCacheType(cacheType)
                .withCacheMaxSize(cacheMaxSize)
                .withCacheExpireMs(cacheExpireMs)
                .withTableName("userInfo")
                .withFieldNames(new String[]{"userId", "userName", "userSex"})
                .withFieldTypes(new TypeInformation[]{Types.STRING, Types.STRING, Types.STRING})
                .withIsAsync(isAsync)
                .build();
    }

    private static void sendToKafka(DataStream<Row> result) {
        DataStream<String> map = result.map((MapFunction<Row, String>) Row::toString);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "172.16.12.155:9094");
        properties.setProperty("retries", "3");
        properties.setProperty(KEY_DISABLE_METRICS, "true");
        map.addSink(new FlinkKafkaProducer010<>("side_table_test", new SimpleStringSchema(StandardCharsets.UTF_8), properties));
    }
}
