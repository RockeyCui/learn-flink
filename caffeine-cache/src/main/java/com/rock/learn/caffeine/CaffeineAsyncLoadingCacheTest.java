package com.rock.learn.caffeine;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

/**
 * @author cuishilei
 * @date 2019/8/23
 */
public class CaffeineAsyncLoadingCacheTest {
    public static void main(String[] args) throws InterruptedException {
        AsyncLoadingCache<Object, Object> asyncLoadingCache = Caffeine.newBuilder()
                //基于时间失效->写入之后开始计时失效
                .expireAfterWrite(2000, TimeUnit.MILLISECONDS)
                //同步加载和手动加载的区别就是在构建缓存时提供一个同步的加载方法
                .buildAsync(new CacheLoader<Object, Object>() {
                    //单个 key 的值加载
                    @Nullable
                    @Override
                    public Object load(@NonNull Object key) throws Exception {
                        System.out.println("---exec load---");
                        return key + "_" + System.currentTimeMillis();
                    }

                    //如果没有重写 loadAll 方法则默认的 loadAll 回循环调用 load 方法，一般重写优化性能
                    @Override
                    public @NonNull Map<Object, Object> loadAll(@NonNull Iterable<?> keys) throws Exception {
                        System.out.println("---exec loadAll---");
                        Map<Object, Object> data = new HashMap<>();
                        for (Object key : keys) {
                            data.put(key, key + "_all_" + System.currentTimeMillis());
                        }
                        return data;
                    }
                });

        String key1 = "key1";

        //获取值，为空则执行异步加载方法
        CompletableFuture<Object> future = asyncLoadingCache.get(key1);
        //异步获取值
        future.thenAccept(new Consumer<Object>() {
            @Override
            public void accept(Object o) {
                System.out.println(System.currentTimeMillis() + "->" + o);
            }
        });
        //批量异步获取
        CompletableFuture<Map<Object, Object>> all = asyncLoadingCache.getAll(Arrays.asList("key1", "key2", "key3"));
        all.thenAccept(new Consumer<Map<Object, Object>>() {
            @Override
            public void accept(Map<Object, Object> objectObjectMap) {
                System.out.println(objectObjectMap);
            }
        });
        sleep(4000);
        ConcurrentMap<Object, CompletableFuture<Object>> asMap = asyncLoadingCache.asMap();
        System.out.println(asMap);
    }
}
