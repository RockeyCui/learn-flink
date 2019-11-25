package com.rock.learn.caffeine.eviction;

import com.github.benmanes.caffeine.cache.*;
import org.checkerframework.checker.index.qual.NonNegative;
import org.checkerframework.checker.nullness.qual.NonNull;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * @author cuishilei
 * @date 2019/10/24
 */
public class SizeBaseTest2 {

    public static void main(String[] args) {
        Cache<Object, Object> cache = Caffeine.newBuilder()
                //缓存最大权重值
                .maximumWeight(150)
                //自定义计算权重
                .weigher(new Weigher<Object, Object>() {
                    @Override
                    public @NonNegative int weigh(@NonNull Object k, @NonNull Object v) {
                        //这里为了简单，直接以 value 为权重
                        return (int) v;
                    }
                })
                .removalListener(new RemovalListener<Object, Object>() {
                    @Override
                    public void onRemoval(@Nullable Object k, @Nullable Object v, @NonNull RemovalCause removalCause) {
                        System.out.println("removed " + k + " cause " + removalCause.toString());
                    }
                })
                .build();

        for (int i = 0; i < 20; i++) {
            cache.put(i, 10);
        }
        cache.cleanUp();
    }
}
