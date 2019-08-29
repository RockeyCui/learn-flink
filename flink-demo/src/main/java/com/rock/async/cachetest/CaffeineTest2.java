package com.rock.async.cachetest;

import com.github.benmanes.caffeine.cache.AsyncLoadingCache;
import com.github.benmanes.caffeine.cache.CacheLoader;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

/**
 * @author cuishilei
 * @date 2019/8/23
 */
public class CaffeineTest2 {
    public static void main(String[] args) throws InterruptedException {
        AsyncLoadingCache<String, Object> asyncCache = Caffeine.newBuilder()
                .refreshAfterWrite(60000, TimeUnit.MILLISECONDS)
                .buildAsync(new CacheLoader<String, Object>() {
                    @Nullable
                    @Override
                    public Object load(@NonNull String s) throws Exception {
                        return s;
                    }
                });
        CompletableFuture<Object> future = asyncCache.get("111");

        future.thenAccept(new Consumer<Object>() {
            @Override
            public void accept(Object o) {
                System.out.println(o);
            }
        });

        sleep(10000);
    }
}
