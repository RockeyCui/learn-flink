package com.rock.sidetable.cache;

import com.rock.sidetable.bean.SideJoinInfo;
import org.apache.flink.calcite.shaded.com.google.common.cache.Cache;
import org.apache.flink.calcite.shaded.com.google.common.cache.CacheBuilder;

import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public class LRUSideCache extends AbstractCache {
    protected transient Cache<String, Object> cache;

    public LRUSideCache(SideJoinInfo sideJoinInfo) {
        super(sideJoinInfo);
    }

    @Override
    public void initCache() {
        Map<String, Object> properties = sideJoinInfo.getProperties();

        Object cacheSize = properties.get("cacheSize");
        Object expireTime = properties.get("expireTime");

        cache = CacheBuilder.newBuilder()
                .maximumSize((Long) cacheSize)
                .expireAfterWrite((Long) expireTime, TimeUnit.MILLISECONDS)
                .build();

    }

    @Override
    public Object get(String key) {
        if (cache == null) {
            return null;
        }
        return cache.getIfPresent(key);
    }

    @Override
    public void put(String key, Object value) {
        if (cache != null) {
            cache.put(key, value);
        }
    }
}
