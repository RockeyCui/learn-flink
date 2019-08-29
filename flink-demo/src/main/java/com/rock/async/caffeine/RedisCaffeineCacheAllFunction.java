package com.rock.async.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.concurrent.TimeUnit;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class RedisCaffeineCacheAllFunction extends RichFlatMapFunction<Row, Row> {
    private static final long serialVersionUID = 7578879189085344807L;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisStringCommands<String, String> commands;

    private Cache<String, Object> cache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        redisClient = RedisClient.create("redis://172.16.44.28:6379/0");
        connection = redisClient.connect();
        commands = connection.sync();

        cache = Caffeine.newBuilder()
                .initialCapacity(1000)
                .maximumSize(10000L)
                .expireAfterWrite(30, TimeUnit.SECONDS)
                .build();
    }

    private Object createExpensiveGraph(String key) {
        return commands.get(key);
    }

    @Override
    public void flatMap(Row row, Collector<Row> collector) {
        String id = String.valueOf(row.getField(0));
        String key = "userInfo:userId:" + id + ":userName";

        Object o = cache.get(key, s -> commands.get(s));
        collector.collect(joinData(row, String.valueOf(o)));
    }

    /**
     * 模拟 join 操作
     *
     * @param input 输入行
     * @return org.apache.flink.types.Row
     * @author cuishilei
     * @date 2019/8/16
     */
    private Row joinData(Row input, String name) {
        int size = input.getArity();
        Row res = new Row(size + 1);
        for (int i = 0; i < size; i++) {
            Object obj = input.getField(i);
            res.setField(i, obj);
        }
        res.setField(size, name);
        return res;
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
    }
}
