package com.rock.async;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.table.typeutils.TimeIndicatorTypeInfo;
import org.apache.flink.types.Row;

import java.sql.Timestamp;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class AsyncFunction extends RichAsyncFunction<Row, Row> {
    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisStringAsyncCommands<String, String> asyncCommands;

    private static final long serialVersionUID = -1113555687079351057L;

    private AsyncCache<String, Object> asyncCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisClient = RedisClient.create("redis://172.16.44.28:6379/0");
        connection = redisClient.connect();
        asyncCommands = connection.async();

        asyncCache = Caffeine.newBuilder()
                .initialCapacity(100)
                .maximumSize(5000L)
                .expireAfterWrite(60000, TimeUnit.MILLISECONDS)
                .buildAsync();
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        String id = String.valueOf(input.getField(0));
        String key = "userInfo:userId:" + id + ":userName";

        CompletableFuture<Object> get = asyncCache.getIfPresent(key);
        if (get == null) {
            RedisFuture<String> stringRedisFuture = asyncCommands.get(key);
            stringRedisFuture.thenAccept(valueRedis -> {
                if (valueRedis != null) {

                    asyncCache.put(key, CompletableFuture.completedFuture(valueRedis));
                    resultFuture.complete(Collections.singleton(joinData(input, valueRedis)));
                } else {
                    resultFuture.complete(Collections.singleton(joinData(input, "xxx")));
                }
            });
        } else {
            get.thenAccept(value -> resultFuture.complete(Collections.singleton(joinData(input, String.valueOf(value)))));
        }
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
            boolean isTimeIndicatorTypeInfo = TimeIndicatorTypeInfo.class.isAssignableFrom(obj.getClass());
            if (obj instanceof Timestamp && isTimeIndicatorTypeInfo) {
                obj = ((Timestamp) obj).getTime();
            }
            res.setField(i, obj);
        }
        res.setField(size, name);
        return res;
    }

    @Override
    public void timeout(Row input, ResultFuture<Row> resultFuture) throws Exception {

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
        if (asyncCache != null) {
            asyncCache = null;
        }
    }
}
