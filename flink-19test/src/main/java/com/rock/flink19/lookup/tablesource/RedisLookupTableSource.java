package com.rock.flink19.lookup.tablesource;

import com.rock.flink19.lookup.function.RedisAsyncTableFunction;
import com.rock.flink19.lookup.function.RedisTableFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.AsyncTableFunction;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.sources.LookupableTableSource;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.utils.TypeConversions;
import org.apache.flink.types.Row;

/**
 * @author cuishilei
 * @date 2019/9/1
 */
public class RedisLookupTableSource implements LookupableTableSource<Row> {

    private final String ip;
    private final int port;
    private final String database;
    private final String username;
    private final String password;
    private final String tableName;
    private final String[] fieldNames;
    private final TypeInformation[] fieldTypes;
    private final boolean isCached;
    private final String cacheType;
    private final long cacheMaxSize;
    private final long cacheExpireMs;
    private final int maxRetryTimes;
    private final boolean isAsync;

    private RedisLookupTableSource(String ip, int port, String database,
                                   String username, String password,
                                   String tableName, String[] fieldNames, TypeInformation[] fieldTypes,
                                   boolean isCached, String cacheType, long cacheMaxSize, long cacheExpireMs, int maxRetryTimes,
                                   boolean isAsync) {
        this.ip = ip;
        this.port = port;
        this.database = database;
        this.username = username;
        this.password = password;
        this.tableName = tableName;
        this.fieldNames = fieldNames;
        this.fieldTypes = fieldTypes;
        this.isCached = isCached;
        this.cacheType = cacheType;
        this.cacheMaxSize = cacheMaxSize;
        this.cacheExpireMs = cacheExpireMs;
        this.maxRetryTimes = maxRetryTimes;
        this.isAsync = isAsync;
    }

    private RedisLookupTableSource(Builder builder) {
        ip = builder.ip;
        port = builder.port;
        database = builder.database;
        username = builder.username;
        password = builder.password;
        tableName = builder.tableName;
        fieldNames = builder.fieldNames;
        fieldTypes = builder.fieldTypes;
        isCached = builder.isCached;
        cacheType = builder.cacheType;
        cacheMaxSize = builder.cacheMaxSize;
        cacheExpireMs = builder.cacheExpireMs;
        maxRetryTimes = builder.maxRetryTimes;
        isAsync = builder.isAsync;
    }

    public static Builder newBuilder() {
        return new Builder();
    }

    @Override
    public TableFunction<Row> getLookupFunction(String[] strings) {
        return RedisTableFunction.newBuilder()
                .withIp(ip)
                .withPort(port)
                .withPassword(password)
                .withDatabase(database)
                .withPassword(password)
                .withTableName(tableName)
                .withFieldNames(fieldNames)
                .withFieldTypes(fieldTypes)
                .withJoinKeyNames(strings)
                .withIsCached(isCached)
                .withCacheType(cacheType)
                .withCacheMaxSize(cacheMaxSize)
                .withCacheExpireMs(cacheExpireMs)
                .withMaxRetryTimes(maxRetryTimes)
                .build();
    }

    @Override
    public AsyncTableFunction<Row> getAsyncLookupFunction(String[] strings) {
        return RedisAsyncTableFunction.newBuilder()
                .withIp(ip)
                .withPort(port)
                .withPassword(password)
                .withDatabase(database)
                .withPassword(password)
                .withTableName(tableName)
                .withFieldNames(fieldNames)
                .withFieldTypes(fieldTypes)
                .withJoinKeyNames(strings)
                .withIsCached(isCached)
                .withCacheMaxSize(cacheMaxSize)
                .withCacheExpireMs(cacheExpireMs)
                .withMaxRetryTimes(maxRetryTimes)
                .build();
    }

    @Override
    public boolean isAsyncEnabled() {
        return isAsync;
    }

    @Override
    public DataType getProducedDataType() {
        return TypeConversions.fromLegacyInfoToDataType(new RowTypeInfo(fieldTypes, fieldNames));
    }

    @Override
    public TableSchema getTableSchema() {
        return TableSchema.builder()
                .fields(fieldNames, TypeConversions.fromLegacyInfoToDataType(fieldTypes))
                .build();
    }

    public static final class Builder {
        private String ip;
        private int port;
        private String database;
        private String username;
        private String password;
        private String tableName;
        private String[] fieldNames;
        private TypeInformation[] fieldTypes;
        private boolean isCached;
        private String cacheType;
        private long cacheMaxSize;
        private long cacheExpireMs;
        private int maxRetryTimes;
        private boolean isAsync;

        private Builder() {
        }

        public Builder withIp(String val) {
            ip = val;
            return this;
        }

        public Builder withPort(int val) {
            port = val;
            return this;
        }

        public Builder withDatabase(String val) {
            database = val;
            return this;
        }

        public Builder withUsername(String val) {
            username = val;
            return this;
        }

        public Builder withPassword(String val) {
            password = val;
            return this;
        }

        public Builder withTableName(String val) {
            tableName = val;
            return this;
        }

        public Builder withFieldNames(String[] val) {
            fieldNames = val;
            return this;
        }

        public Builder withFieldTypes(TypeInformation[] val) {
            fieldTypes = val;
            return this;
        }

        public Builder withIsCached(boolean val) {
            isCached = val;
            return this;
        }

        public Builder withCacheType(String val) {
            cacheType = val;
            return this;
        }

        public Builder withCacheMaxSize(long val) {
            cacheMaxSize = val;
            return this;
        }

        public Builder withCacheExpireMs(long val) {
            cacheExpireMs = val;
            return this;
        }

        public Builder withMaxRetryTimes(int val) {
            maxRetryTimes = val;
            return this;
        }

        public Builder withIsAsync(boolean val) {
            isAsync = val;
            return this;
        }

        public RedisLookupTableSource build() {
            return new RedisLookupTableSource(this);
        }
    }
}
