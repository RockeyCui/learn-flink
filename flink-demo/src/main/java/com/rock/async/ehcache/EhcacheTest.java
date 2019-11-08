package com.rock.async.ehcache;

import org.ehcache.Cache;
import org.ehcache.CachePersistenceException;
import org.ehcache.UserManagedCache;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.builders.UserManagedCacheBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.ehcache.core.spi.service.LocalPersistenceService;
import org.ehcache.impl.config.persistence.DefaultPersistenceConfiguration;
import org.ehcache.impl.config.persistence.UserManagedPersistenceContext;
import org.ehcache.impl.persistence.DefaultLocalPersistenceService;

import java.io.File;

/**
 * @author cuishilei
 * @date 2019/8/29
 */
public class EhcacheTest {
    public static void main(String[] args) throws CachePersistenceException {

        LocalPersistenceService persistenceService = new DefaultLocalPersistenceService(
                new DefaultPersistenceConfiguration(new File("D:\\cache")));
        UserManagedCache<String, String> cache = UserManagedCacheBuilder
                .newUserManagedCacheBuilder(String.class, String.class)
                .with(new UserManagedPersistenceContext<>("persistentCache", persistenceService))
                .withResourcePools(ResourcePoolsBuilder.newResourcePoolsBuilder()
                                .heap(1L, MemoryUnit.MB)
                        //.heap(100L, MemoryUnit.MB)
                        //.offheap(200L, MemoryUnit.MB)
                        //.disk(300L, MemoryUnit.MB, false)
                ).build(true);


        for (int i = 0; i < 10100; i++) {
            cache.put(i + "k", i + "v");
        }
        int count = 0;
        for (Cache.Entry<String, String> stringStringEntry : cache) {
            System.out.println(stringStringEntry.getKey());
            count++;
        }
        System.out.println(count);
    }
}
