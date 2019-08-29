package com.rock.async.caffeine;

import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.TimeUnit;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class RedisCaffeineLoadingCacheAllFunction extends RichFlatMapFunction<Row, Row> {
    private static final long serialVersionUID = 7578879189085344807L;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisStringCommands<String, String> commands;

    private LoadingCache<String, Object> loadingCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        redisClient = RedisClient.create("redis://172.16.44.28:6379/0");
        connection = redisClient.connect();
        commands = connection.sync();

        loadingCache = Caffeine.newBuilder()
                .refreshAfterWrite(30, TimeUnit.SECONDS)
                .build(new CacheLoader<String, Object>() {
                    @Nullable
                    @Override
                    public Object load(@NonNull String s) {
                        return createExpensiveGraph(s);
                    }
                });
    }

    private Object createExpensiveGraph(String key) {
        return commands.get(key);
    }

    @Override
    public void flatMap(Row row, Collector<Row> collector) {
        String id = String.valueOf(row.getField(0));
        String key = "userInfo:userId:" + id + ":userName";
        collector.collect(joinData(row, String.valueOf(loadingCache.get(key))));
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
