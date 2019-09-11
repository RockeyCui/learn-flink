package com.rock.learn.caffeine;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static java.lang.Thread.sleep;

/**
 * @author cuishilei
 * @date 2019/8/23
 */
public class CaffeineCacheTest {
    public static void main(String[] args) throws InterruptedException {
        Cache<Object, Object> cache = Caffeine.newBuilder()
                //基于时间失效->写入之后开始计时失效
                .expireAfterWrite(2000, TimeUnit.MILLISECONDS)
                .build();

        //返回 key + 当前时间戳作为 value
        Function<Object, Object> getFuc = key -> key + "_" + System.currentTimeMillis();

        String key1 = "key1";

        //获取 key1 对应的值，如果获取不到则执行 getFuc
        Object value = cache.get(key1, getFuc);
        System.out.println(value);

        sleep(2001);

        //获取 key1 对应的值，如果获取不到则返回 null
        value = cache.getIfPresent(key1);
        System.out.println(value);

        //设置 key1 的值
        cache.put(key1, "putValue");
        value = cache.get(key1, getFuc);
        System.out.println(value);

        ConcurrentMap<Object, Object> asMap = cache.asMap();
        System.out.println(asMap);

        //删除 key1
        cache.invalidate(key1);
        asMap = cache.asMap();
        System.out.println(asMap);
    }
}
