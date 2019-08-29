package com.rock.flink19.lookup;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

/**
 * @author cuishilei
 * @date 2019/8/27
 */
public class LookUpTableTest {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment bsEnv = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings bsSettings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
        StreamTableEnvironment bsTableEnv = StreamTableEnvironment.create(bsEnv, bsSettings);


        String sqlCreate = "create table\n" +
                "  redis_output (\n" +
                "  a varchar,\n" +
                "  b varchar,\n" +
                "  primary key(a)\n" +
                ") with (\n" +
                "  connector.type = 'redis',\n" +
                "  mode = 'string',\n" +
                "  host = '172.16.44.28',\n" +
                "  port = '6379',\n" +
                "  dbNum = '0',\n" +
                "  ignoreDelete = 'true'\n" +
                ")";
        bsTableEnv.sqlUpdate(sqlCreate);

        bsTableEnv.execute("LookUpTableTest");
    }

}
