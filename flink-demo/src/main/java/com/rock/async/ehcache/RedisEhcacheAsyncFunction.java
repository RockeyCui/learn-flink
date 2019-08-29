package com.rock.async.ehcache;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisStringAsyncCommands;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;
import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.ExpiryPolicyBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;

import java.time.Duration;
import java.util.Collections;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class RedisEhcacheAsyncFunction extends RichAsyncFunction<Row, Row> {
    private RedisClient redisClient;

    private StatefulRedisConnection<String, String> connection;

    private RedisStringAsyncCommands<String, String> asyncCommands;

    private static final long serialVersionUID = -1113555687079351057L;

    private UserManagedCache<String, String> userManagedCache;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        redisClient = RedisClient.create("redis://172.16.44.28:6379/0");
        connection = redisClient.connect();
        asyncCommands = connection.async();

        userManagedCache = UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, String.class)
                .withResourcePools(ResourcePoolsBuilder
                        .heap(5000))
                .withExpiry(ExpiryPolicyBuilder.timeToLiveExpiration(Duration.ofSeconds(30)))
                .build(true);
    }

    @Override
    public void asyncInvoke(Row input, ResultFuture<Row> resultFuture) {
        String id = String.valueOf(input.getField(0));
        String key = "userInfo:userId:" + id + ":userName";
        String s = userManagedCache.get(key);
        if (s != null) {
            resultFuture.complete(Collections.singleton(joinData(input, s)));
        } else {
            RedisFuture<String> stringRedisFuture = asyncCommands.get(key);
            stringRedisFuture.thenAccept(valueRedis -> {
                resultFuture.complete(Collections.singleton(joinData(input, valueRedis)));
                userManagedCache.put(key, valueRedis);
            });
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
        if (userManagedCache != null) {
            userManagedCache.close();
        }
    }
}
