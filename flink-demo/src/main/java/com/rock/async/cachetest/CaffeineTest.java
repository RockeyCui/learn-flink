package com.rock.async.cachetest;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import static java.lang.Thread.sleep;

/**
 * @author cuishilei
 * @date 2019/8/23
 */
public class CaffeineTest {
    public static void main(String[] args) throws InterruptedException {
        AsyncCache<String, Object> asyncCache = Caffeine.newBuilder()
                .initialCapacity(2000)
                .maximumSize(10000L)
                .expireAfterWrite(60000, TimeUnit.MILLISECONDS)
                .buildAsync();

        asyncCache.put("111", CompletableFuture.completedFuture("222"));

        CompletableFuture<Object> ifPresent = asyncCache.getIfPresent("111");

        if (ifPresent == null) {
            System.out.println("null");
        } else {
            ifPresent.thenAccept(new Consumer<Object>() {
                @Override
                public void accept(Object o) {
                    System.out.println("o=>" + o);
                }
            });
        }


        sleep(10000);
    }
}
