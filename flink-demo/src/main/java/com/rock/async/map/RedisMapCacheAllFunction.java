package com.rock.async.map;

import io.lettuce.core.KeyValue;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisKeyCommands;
import io.lettuce.core.api.sync.RedisStringCommands;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.calcite.shaded.com.google.common.collect.Maps;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.types.Row;
import org.apache.flink.util.Collector;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class RedisMapCacheAllFunction extends RichFlatMapFunction<Row, Row> {
    private static final long serialVersionUID = 7578879189085344807L;

    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisStringCommands<String, String> commands;

    private RedisKeyCommands<String, String> keyCommands;

    private AtomicReference<Map<String, String>> cacheRef = new AtomicReference<>();

    private ScheduledExecutorService es;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        redisClient = RedisClient.create("redis://172.16.44.28:6379/0");
        connection = redisClient.connect();
        commands = connection.sync();
        keyCommands = connection.sync();

        reloadCache();
        es = Executors.newSingleThreadScheduledExecutor(new MyThreadFactory("cache-all-reload"));
        es.scheduleAtFixedRate(this::reloadCache, 30000, 30000, TimeUnit.MILLISECONDS);

    }

    private void reloadCache() {
        Map<String, String> newCache = Maps.newConcurrentMap();
        List<String> keys = keyCommands.keys("userInfo:userId:*:userName");
        List<KeyValue<String, String>> mget = commands.mget(keys.toArray(new String[]{}));
        for (KeyValue<String, String> keyValue : mget) {
            newCache.put(keyValue.getKey(), keyValue.getValue());
        }
        cacheRef.set(newCache);
    }

    @Override
    public void flatMap(Row row, Collector<Row> collector) {
        String id = String.valueOf(row.getField(0));
        String key = "userInfo:userId:" + id + ":userName";

        Map<String, String> cacheMap = cacheRef.get();
        String value = cacheMap.get(key);

        collector.collect(joinData(row, String.valueOf(value)));
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
