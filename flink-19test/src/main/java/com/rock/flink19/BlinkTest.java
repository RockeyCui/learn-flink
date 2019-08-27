package com.rock.flink19;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @author cuishilei
 * @date 2019/8/26
 */
public class BlinkTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);

        DataStream<String> source = bsEnv.readTextFile("hdfs://172.16.44.28:8020/flink/userClick_Random_1Y", "UTF-8");

        TypeInformation[] types = new TypeInformation[]{Types.STRING, Types.STRING, Types.LONG};
        String[] fields = new String[]{"id", "user_click", "time"};
        RowTypeInfo rowTypeInfo = new RowTypeInfo(types, fields);

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
        }).returns(rowTypeInfo);

        bsTableEnv.registerDataStream("user_click", stream, String.join(",", rowTypeInfo.getFieldNames()));
        String sql1 = "select * from user_click";
        Table tableQuery = bsTableEnv.sqlQuery(sql1);
        DataStream<Row> result = bsTableEnv.toAppendStream(tableQuery, Row.class);
        result.print();

        bsEnv.execute("user_click");
    }
}
