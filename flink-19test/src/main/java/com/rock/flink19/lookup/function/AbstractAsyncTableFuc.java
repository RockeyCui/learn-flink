package com.rock.flink19.lookup.function;

import com.github.benmanes.caffeine.cache.AsyncCache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.google.common.collect.Sets;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.types.Row;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * @author cuishilei
 * @date 2019/9/1
 */
public abstract class AbstractAsyncTableFuc extends AsyncTableFunction<Row> {
    private static final long serialVersionUID = -6654639067122929406L;

    protected final String[] fieldNames;
    protected final TypeInformation[] fieldTypes;
    protected final String[] joinKeyNames;
    protected final boolean isCached;
    protected final String cacheType;
    protected final long cacheMaxSize;
    protected final long cacheExpireMs;
    protected final int maxRetryTimes;

    protected int[] joinKeyIndexes;
    protected String[] otherFieldNames;
    protected int[] otherFieldNamesIndexes;

    protected transient AsyncCache<String, Row> asyncCache;

    public AbstractAsyncTableFuc(String[] fieldNames, TypeInformation[] fieldTypes, String[] joinKeyNames,
                                 boolean isCached, String cacheType, long cacheMaxSize, long cacheExpireMs,
                                 int maxRetryTimes) {
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.joinKeyNames = joinKeyNames;
        this.isCached = isCached;
        this.cacheType = cacheType;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;

        List<String> fieldNamesList = Arrays.asList(fieldNames);
        //找出主键索引
        joinKeyIndexes = FieldUtil.getFieldIndexes(fieldNames, joinKeyNames);
        //找出非主键的字段的索引
        otherFieldNames = Sets.difference(Sets.newHashSet(fieldNames), Sets.newHashSet(joinKeyNames))
                .toArray(new String[]{});
        otherFieldNamesIndexes = FieldUtil.getFieldIndexes(fieldNames, otherFieldNames);
    }

    protected void initCache() {
        asyncCache = Caffeine.newBuilder()
                .maximumSize(cacheMaxSize)
                .expireAfterWrite(cacheExpireMs, TimeUnit.MILLISECONDS)
                .buildAsync();
    }

    /**
     * 返回需要的 row
     *
     * @param data 定制参数
     * @return org.apache.flink.types.Row
     * @author cuishilei
     * @date 2019/9/1
     */
    abstract Row getRow(Object... data);

    @Override
    public TypeInformation<Row> getResultType() {
        return new RowTypeInfo(fieldTypes, fieldNames);
    }
}
