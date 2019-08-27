package com.rock.sidetable;

import com.rock.sidetable.bean.SideJoinInfo;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisKeyAsyncCommands;
import io.lettuce.core.cluster.RedisClusterClient;
import io.lettuce.core.cluster.api.StatefulRedisClusterConnection;
import org.apache.flink.calcite.shaded.com.google.common.collect.Lists;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.types.Row;

import java.util.List;
import java.util.Map;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class RedisSideTableJoinFunc extends AbstractSideTableJoinFunc {
    private static final long serialVersionUID = 315022259104060922L;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisClusterClient redisClusterClient;

    private RedisKeyAsyncCommands<String, String> async;

    private StatefulRedisClusterConnection<String, String> redisClusterConnection;

    public RedisSideTableJoinFunc(SideJoinInfo sideJoinInfo) {
        super(sideJoinInfo);
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        if (sideJoinInfo.getJoinCol() == null || sideJoinInfo.getJoinCol().size() < 1) {
            throw new RuntimeException("equal col is null");
        }
        if (sideJoinInfo.getJoinColIndex() == null || sideJoinInfo.getJoinColIndex().size() < 1) {
            throw new RuntimeException("equal col index is null");
        }
        if (sideJoinInfo.getJoinColIndex().size() != sideJoinInfo.getJoinCol().size()) {
            throw new RuntimeException("equal col index is illegal");
        }
        super.open(parameters);
        this.buildRedisClient(sideJoinInfo.getProperties());
    }

    @Override
    public Row joinData(Row input, Object sideTableData) {

        return null;
    }

    @Override
    public void asyncInvoke(Row row, ResultFuture<Row> resultFuture) throws Exception {
        List<String> keys = Lists.newLinkedList();

        for (int i = 0; i < sideJoinInfo.getJoinCol().size(); i++) {

        }
    }

    private void buildRedisClient(Map<String, Object> properties) {
        String url = properties.get("url") == null ? null : String.valueOf(properties.get("url"));
        String password = properties.get("password") == null ? null : String.valueOf(properties.get("password"));
        if (password != null) {
            password = password + "@";
        } else {
            password = "";
        }
        String database = properties.get("database") == null ? null : String.valueOf(properties.get("database"));
        if (database == null) {
            database = "0";
        }
        Integer redisType = properties.get("redisType") == null ? null : (Integer) properties.get("redisType");
        switch (redisType) {
            case 1:
                redisClient = RedisClient.create("redis://" + password + url + "/" + database);
                connection = redisClient.connect();
                async = connection.async();
                break;
            case 2:
                redisClusterClient = RedisClusterClient.create("redis://" + password + url);
                redisClusterConnection = redisClusterClient.connect();
                async = redisClusterConnection.async();
                break;
            default:
                throw new RuntimeException();
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) {
            connection.close();
        }
        if (redisClient != null) {
            redisClient.shutdown();
        }
        if (redisClusterConnection != null) {
            redisClusterConnection.close();
        }
        if (redisClusterClient != null) {
            redisClusterClient.shutdown();
        }
    }
}
