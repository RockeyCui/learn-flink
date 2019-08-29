package com.rock.async.cachetest;

import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.MemoryUnit;

/**
 * @author cuishilei
 * @date 2019/8/29
 */
public class EhcacheTest {
    public static void main(String[] args) {
        CacheManager cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
                .withCache("sideTableCache",
                        CacheConfigurationBuilder.newCacheConfigurationBuilder(String.class, String.class,
                                ResourcePoolsBuilder
                                        .heap(1000)
                                        .offheap(10, MemoryUnit.MB)))
                .build(true);

        Cache<String, String> sideTableCache =
                cacheManager.getCache("sideTableCache", String.class, String.class);
        sideTableCache.put("111", "222");

        String s = sideTableCache.get("111");

        System.out.println(s);

        cacheManager.close();


        UserManagedCache<String, String> userManagedCache =
                UserManagedCacheBuilder.newUserManagedCacheBuilder(String.class, String.class)
                        .withResourcePools(ResourcePoolsBuilder
                                .heap(10000)
                                .offheap(10, MemoryUnit.MB))
                        .build(true);

        userManagedCache.close();
    }
}
