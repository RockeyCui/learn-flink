package com.rock.learn.caffeine.eviction;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * @author cuishilei
 * @date 2019/10/24
 */
public class SizeBaseTest1 {

    public static void main(String[] args) {
        Cache<Object, Object> cache = Caffeine.newBuilder()
                //缓存最大条数,超过这个条数就是驱逐缓存
                .maximumSize(20)
                .removalListener(new RemovalListener<Object, Object>() {
                    @Override
                    public void onRemoval(@Nullable Object k, @Nullable Object v, @NonNull RemovalCause removalCause) {
                        System.out.println("removed " + k + " cause " + removalCause.toString());
                    }
                })
                .build();

        for (int i = 0; i < 25; i++) {
            cache.put(i, i + "_value");
        }
        cache.cleanUp();
    }
}
