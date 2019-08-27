package com.rock.sidetable;

import com.rock.sidetable.bean.SideJoinInfo;
import com.rock.sidetable.cache.AbstractCache;
import com.rock.sidetable.cache.LRUSideCache;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
import org.apache.flink.types.Row;

/**
 * @author cuishilei
 * @date 2019/8/16
 */
public abstract class AbstractSideTableJoinFunc extends RichAsyncFunction<Row, Row> implements SideTableJoinFunc {

    private static final long serialVersionUID = 1236086029723182751L;

    protected SideJoinInfo sideJoinInfo;

    AbstractSideTableJoinFunc(SideJoinInfo sideJoinInfo) {
        this.sideJoinInfo = sideJoinInfo;
    }


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        this.initCache();
    }

    private void initCache() {
        String cacheType = (String) sideJoinInfo.getFromProperties("cacheType");
        if (cacheType != null) {
            AbstractCache cache;
            if ("LRU".equalsIgnoreCase(cacheType)) {
                cache = new LRUSideCache(sideJoinInfo);
                sideJoinInfo.setCache(cache);
            } else {
                throw new RuntimeException("not support side cache with type:" + cacheType);
            }
            cache.initCache();
        }
    }

    protected Object getFromCache(String key) {
        return sideJoinInfo.getCache().get(key);
    }

    protected void putCache(String key, Object value) {
        sideJoinInfo.getCache().put(key, value);
    }

    protected boolean cacheOpen() {
        return sideJoinInfo.getCache() != null;
    }

}
